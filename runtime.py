"""MQTT channel: connects to a mosquitto broker for agent-to-agent messaging.

Uses paho-mqtt in a background thread with auto-reconnect, so the connection
stays alive long-term without blocking the agent's main loop.

Agent identity: the MQTT-visible name is always ``ag_<agent_id>`` (prefix
auto-added unless agent_id already starts with it). Set ``username`` only to
override the MQTT auth account explicitly.

Topics (single ``im`` namespace, ``topic_prefix`` defaulting to ``im``):
  - Subscribe: {prefix}/ag_<name>/inbox
  - Publish:   {prefix}/<sender>/inbox   (reply to a human — no countersign)
               {prefix}/<ag_target>/inbox (reply to an agent — countersign sent)

Agent-directed messages (any ``ag_*`` target) carry our ``countersign``
(16-char code issued at agent creation); replies to humans never carry it.
Inbound countersign verification is the receiving agent's responsibility
(payload-level convention; the broker does not inspect payloads).

Set ``tls: true`` for TLS brokers (e.g. Fireside production on port 8883).

Payload format matches the Fireside webim envelope:
  {"from": ..., "display_name": ..., "msg_type": "im",
   "content_type": "text", "text": ..., "ts": ...}

Images/files follow the Fireside webim media spec:
  - JSON inline:  content_type "image" + base64 ``image_data`` (< 70 KB)
  - Binary frame: ``1B ver | 1B type | 2B meta_len (BE) | meta JSON | data``
    type 0x01=TEXT, 0x02=IMAGE (<= 2 MB), 0x03=FILE (<= 10 MB), 0x04=SYSTEM
  - Detection: buf[0]==0x01 -> binary frame, 0x7B ('{') -> JSON, else raw text.
Received media is saved under the ``media/mqtt`` runtime dir and passed to the
agent loop via ``InboundMessage.media``.
"""

from __future__ import annotations

import asyncio
import base64
import json
import re
import struct
import time
import uuid
from pathlib import Path
from typing import Any

from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base

# Fireside binary protocol v1 frame types
FRAME_TEXT = 0x01
FRAME_IMAGE = 0x02
FRAME_FILE = 0x03
FRAME_SYSTEM = 0x04

# Client-side size limits the spec says clients MUST enforce
INLINE_IMAGE_LIMIT = 70 * 1024  # base64 JSON threshold
MAX_IMAGE_BYTES = 2 * 1024 * 1024
MAX_FILE_BYTES = 10 * 1024 * 1024

_MIME_BY_EXT = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
}
_EXT_BY_MIME = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
}
_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")

MQTT_AVAILABLE = False
mqtt: Any = None

try:
    import paho.mqtt as _mqtt_pkg  # noqa: F401
    import paho.mqtt.client as mqtt  # type: ignore[no-redef]

    MQTT_AVAILABLE = True
except ImportError:
    pass


class MQTTConfig(Base):
    """MQTT channel configuration.

    - ``broker_host`` / ``broker_port``: mosquitto endpoint.
    - ``agent_id``: this agent's identity; the MQTT name is ``ag_<agent_id>``
      and the inbound topic is ``{topic_prefix}/ag_<agent_id>/inbox``.
    - ``username`` / ``password``: optional broker credentials; leave empty
      for anonymous brokers.
    - ``transport``: ``tcp`` (default) or ``ws`` for MQTT over WebSocket.
    - ``tls``: enable TLS (CERT_REQUIRED) — required for Fireside production
      (mqtt.openiot.co:8883).
    - ``topic_prefix``: human inbox namespace, default ``im``.
    - ``countersign``: 16-char agent countersign; attached to outbound
      agent-directed messages (``ag_*`` targets) for the receiver to verify.
    - ``display_name``: sender display name on outbound payloads (defaults to
      the ``ag_<agent_id>`` identity).
    - ``allow_from``: sender ids allowed to talk to the agent; use ``["*"]``
      to allow everyone (empty denies all senders, per nanobot policy).
    """

    enabled: bool = False
    broker_host: str = "127.0.0.1"
    broker_port: int = 1883
    agent_id: str = ""
    username: str = ""
    password: str = ""
    transport: str = "tcp"
    tls: bool = False
    topic_prefix: str = "im"
    countersign: str = ""
    display_name: str = ""
    allow_from: list[str] = Field(default_factory=list)


class MQTTChannel(BaseChannel):
    """MQTT channel that connects to a mosquitto broker.

    paho-mqtt runs in a background thread (``loop_start`` with
    ``reconnect_on_failure``), so the connection is maintained long-term.
    """

    name = "mqtt"
    display_name = "MQTT"

    def __init__(self, config: Any, bus: MessageBus):
        if isinstance(config, dict):
            config = MQTTConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: MQTTConfig = config
        self._client: Any = None
        self._connected = False
        self._loop: asyncio.AbstractEventLoop | None = None

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return MQTTConfig().model_dump(by_alias=True)

    # -- identity helpers ----------------------------------------------------

    @property
    def _sender_name(self) -> str:
        return self.config.display_name or self.identity

    @property
    def identity(self) -> str:
        """MQTT-visible agent name: always ag_<agent_id> (prefix auto-added)."""
        aid = self.config.agent_id
        return aid if aid.startswith("ag_") else f"ag_{aid}"

    @staticmethod
    def _is_agent_name(name: str) -> bool:
        return str(name).startswith("ag_")

    # -- paho callbacks (run in paho's background thread) -------------------

    def _on_connect(self, client: Any, userdata: Any, flags: Any, rc: Any, properties: Any = None) -> None:
        if rc == 0:
            self._connected = True
            human_inbox = f"{self.config.topic_prefix}/{self.identity}/inbox"
            client.subscribe(human_inbox, qos=1)
            self.logger.info(
                "connected to {}:{} — subscribed to {}",
                self.config.broker_host,
                self.config.broker_port,
                human_inbox,
            )
        else:
            self.logger.error(
                'broker rejected connection (rc={}) username="{}" broker={}:{}',
                rc,
                self.config.username,
                self.config.broker_host,
                self.config.broker_port,
            )

    def _on_disconnect(self, client: Any, userdata: Any, flags: Any, rc: Any, properties: Any = None) -> None:
        self._connected = False
        if rc != 0:
            self.logger.warning("unexpected disconnect (rc={}) — auto-reconnect will retry", rc)
        else:
            self.logger.info("disconnected")

    # -- payload parsing (Fireside webim: JSON + binary frame v1) ------------

    @staticmethod
    def _build_frame(ftype: int, meta: dict[str, Any], data: bytes | None = None) -> bytes:
        """Build a Fireside binary frame: 1B ver | 1B type | 2B meta_len | meta | data."""
        meta_bytes = json.dumps(meta, ensure_ascii=False).encode("utf-8")
        return struct.pack(">BBH", 1, ftype, len(meta_bytes)) + meta_bytes + (data or b"")

    def _parse_payload(self, raw: bytes, topic: str) -> dict[str, Any] | None:
        """Normalize any Fireside payload into a dict, or None to drop."""
        if not raw:
            self.logger.warning("empty payload on {}", topic)
            return None
        if raw[0] == 0x01:  # binary frame (ver=1)
            return self._parse_binary_frame(raw, topic)
        if raw[0] == 0x7B:  # '{' — legacy JSON envelope
            return self._parse_json_payload(raw, topic)
        self.logger.warning(
            "unsupported raw payload on {} ({!r})", topic, raw[:40]
        )
        return None

    def _parse_json_payload(self, raw: bytes, topic: str) -> dict[str, Any] | None:
        try:
            data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            self.logger.warning("invalid JSON on {}: {!r}", topic, raw[:80])
            return None
        if not isinstance(data, dict):
            self.logger.warning("ignoring non-object payload on {}", topic)
            return None
        sender = str(data.get("from", "")).strip()
        if not sender:
            self.logger.warning("message missing 'from' on {}", topic)
            return None

        content_type = str(data.get("content_type", "text"))
        parsed: dict[str, Any] = {
            "from": sender,
            "display_name": data.get("display_name", sender),
            "text": str(data.get("text", "")),
            "content_type": content_type,
            "ts": data.get("ts"),
            "countersign": str(data.get("countersign", "")),
            "media_bytes": None,
            "media_name": None,
        }
        if content_type == "image" and data.get("image_data"):
            try:
                parsed["media_bytes"] = base64.b64decode(str(data["image_data"]), validate=True)
            except Exception:
                self.logger.warning("bad base64 image_data on {} — ignoring media", topic)
            mime = str(data.get("mime", "image/jpeg"))
            ext = _EXT_BY_MIME.get(mime, ".jpg")
            parsed["media_name"] = f"image{ext}"
            parsed["mime"] = mime
        elif content_type in ("image", "file") and data.get("url"):
            self.logger.warning(
                "url-based {} on {} not implemented in this channel — text only",
                content_type,
                topic,
            )
        return parsed

    def _parse_binary_frame(self, raw: bytes, topic: str) -> dict[str, Any] | None:
        if len(raw) < 4:
            self.logger.warning("truncated binary frame on {}", topic)
            return None
        _ver, ftype = raw[0], raw[1]
        (meta_len,) = struct.unpack(">H", raw[2:4])
        if len(raw) < 4 + meta_len:
            self.logger.warning("truncated meta in binary frame on {}", topic)
            return None
        try:
            meta = json.loads(raw[4 : 4 + meta_len].decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            self.logger.warning("invalid meta JSON in binary frame on {}", topic)
            return None
        if not isinstance(meta, dict):
            self.logger.warning("ignoring non-object frame meta on {}", topic)
            return None
        sender = str(meta.get("from", "")).strip()
        if not sender:
            self.logger.warning("binary frame missing 'from' on {}", topic)
            return None

        data = raw[4 + meta_len :]
        parsed: dict[str, Any] = {
            "from": sender,
            "display_name": meta.get("display_name", sender),
            "text": str(meta.get("text", "")),
            "content_type": "text",
            "ts": meta.get("ts"),
            "countersign": str(meta.get("countersign", "")),
            "media_bytes": None,
            "media_name": None,
        }
        if ftype == FRAME_IMAGE:
            if len(data) > MAX_IMAGE_BYTES:
                self.logger.warning("image frame too large ({} B) on {}", len(data), topic)
                return None
            mime = str(meta.get("mime", "image/jpeg"))
            ext = _EXT_BY_MIME.get(mime, ".jpg")
            parsed.update(content_type="image", media_bytes=data, media_name=f"image{ext}", mime=mime)
        elif ftype == FRAME_FILE:
            if len(data) > MAX_FILE_BYTES:
                self.logger.warning("file frame too large ({} B) on {}", len(data), topic)
                return None
            filename = str(meta.get("filename", "file.bin"))
            parsed.update(content_type="file", media_bytes=data, media_name=filename)
        elif ftype == FRAME_SYSTEM:
            parsed["content_type"] = "system"
        elif ftype != FRAME_TEXT:
            self.logger.warning("unknown frame type {:#04x} on {}", ftype, topic)
            return None
        return parsed

    def _save_media(self, parsed: dict[str, Any]) -> str | None:
        """Persist received media bytes under the mqtt media dir (blocking; run in thread)."""
        try:
            media_dir = get_media_dir("mqtt")
            safe_name = _SAFE_NAME_RE.sub("_", str(parsed.get("media_name") or "media"))[:80]
            # uuid suffix: ms-timestamp alone can collide when bursts land in
            # the same millisecond and to_thread completion order is undefined.
            path = media_dir / f"{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}_{safe_name}"
            path.write_bytes(parsed["media_bytes"])
            return str(path)
        except OSError as exc:
            self.logger.warning("failed to save media: {}", exc)
            return None

    async def _handle_inbound(self, parsed: dict[str, Any], topic: str) -> None:
        """Async side of inbound handling: save media, then hand to BaseChannel."""
        if self._loop is None or self._loop.is_closed():
            self.logger.warning("event loop not available — dropping message")
            return

        media_paths: list[str] = []
        if parsed.get("media_bytes"):
            path = await asyncio.to_thread(self._save_media, parsed)
            if path:
                media_paths.append(path)

        content = parsed["text"]
        if not content and media_paths:
            label = "image" if parsed["content_type"] == "image" else "file"
            content = f"[{label} received]"

        await self._handle_message(
            sender_id=parsed["from"],
            chat_id=parsed["from"],
            content=content,
            media=media_paths or None,
            metadata={
                "topic": topic,
                "sender_name": parsed["display_name"],
                "content_type": parsed["content_type"],
                "ts": parsed["ts"],
            },
            is_dm=True,
        )

    def _on_message(self, client: Any, userdata: Any, msg: Any) -> None:
        """Receive a message addressed to this agent."""
        parsed = self._parse_payload(msg.payload, msg.topic)
        if parsed is None:
            return
        # Ignore our own echoes (e.g. shared wildcard subscriptions).
        if parsed["from"] == self.identity:
            return

        if self._loop is None or self._loop.is_closed():
            self.logger.warning("event loop not available — dropping message")
            return

        coro = self._handle_inbound(parsed, msg.topic)
        asyncio.run_coroutine_threadsafe(coro, self._loop)

    # -- BaseChannel interface ----------------------------------------------

    async def start(self) -> None:
        """Connect to the broker and keep the connection alive."""
        if not MQTT_AVAILABLE:
            self.logger.error(
                "paho-mqtt is not installed. Run: pip install 'paho-mqtt>=2.1.0'"
            )
            return
        if not self.config.agent_id:
            self.logger.error("agent_id is required for the MQTT channel")
            return

        self._running = True
        self._loop = asyncio.get_running_loop()

        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"agent_{self.config.agent_id}",
            transport=self.config.transport,
            reconnect_on_failure=True,
        )
        if self.config.tls:
            import ssl as _ssl

            self._client.tls_set(cert_reqs=_ssl.CERT_REQUIRED)
        if self.config.username:
            self._client.username_pw_set(self.config.username, self.config.password or None)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        self.logger.info(
            "connecting to {}:{} (agent={}, tls={})...",
            self.config.broker_host,
            self.config.broker_port,
            self.config.agent_id,
            self.config.tls,
        )
        self._client.connect(self.config.broker_host, self.config.broker_port, 60)
        self._client.loop_start()

        # Long-running: keep alive until stop() is called.
        try:
            while self._running:
                await asyncio.sleep(1)
        finally:
            self._running = False

    async def stop(self) -> None:
        """Disconnect and clean up."""
        self._running = False
        if self._client is not None:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception as exc:
                self.logger.warning("disconnect error: {}", exc)
            self._client = None
        self._connected = False
        self.logger.info("channel stopped")

    def _build_media_payload(self, msg: OutboundMessage, media_path: str, to_agent: bool = False) -> tuple[bytes, str] | None:
        """Build a Fireside payload for one media file. Returns (payload, kind)."""
        path = Path(media_path)
        if not path.is_file():
            self.logger.warning("media file not found: {}", media_path)
            return None
        data = path.read_bytes()
        mime = _MIME_BY_EXT.get(path.suffix.lower(), "application/octet-stream")
        meta = {
            "from": self.identity,
            "display_name": self._sender_name,
            "msg_type": "im",
            "ts": int(time.time() * 1000),
        }
        if to_agent and self.config.countersign:
            meta["countersign"] = self.config.countersign
        if mime.startswith("image/"):
            if len(data) > MAX_IMAGE_BYTES:
                self.logger.warning("refusing to send image over 2 MB: {}", media_path)
                return None
            if len(data) < INLINE_IMAGE_LIMIT:
                payload = json.dumps(
                    {
                        **meta,
                        "content_type": "image",
                        "image_data": base64.b64encode(data).decode("ascii"),
                        "mime": mime,
                        "size": len(data),
                    },
                    ensure_ascii=False,
                ).encode("utf-8")
                return payload, "image(json)"
            frame_meta = {**meta, "mime": mime, "size": len(data)}
            return self._build_frame(FRAME_IMAGE, frame_meta, data), "image(frame)"
        if len(data) > MAX_FILE_BYTES:
            self.logger.warning("refusing to send file over 10 MB: {}", media_path)
            return None
        frame_meta = {**meta, "filename": path.name, "size": len(data)}
        return self._build_frame(FRAME_FILE, frame_meta, data), "file(frame)"

    async def send(self, msg: OutboundMessage) -> None:
        """Publish a reply (text + optional media) back to the sender's inbox.

        Routing: everything goes to {topic_prefix}/<target>/inbox; replies
        to agents (ag_*) carry our countersign, replies to humans don't.
        """
        if self._client is None or not self._connected:
            raise RuntimeError("MQTT broker is not connected")

        to_agent = self._is_agent_name(msg.chat_id)
        topic = f"{self.config.topic_prefix}/{msg.chat_id}/inbox"
        if msg.content:
            body: dict[str, Any] = {
                "from": self.identity,
                "display_name": self._sender_name,
                "msg_type": "im",
                "content_type": "text",
                "text": msg.content,
                "ts": int(time.time() * 1000),
            }
            if to_agent and self.config.countersign:
                body["countersign"] = self.config.countersign
            payload = json.dumps(body, ensure_ascii=False)
            result = self._client.publish(topic, payload, qos=1)
            if result.rc != 0:
                raise RuntimeError(f"MQTT publish to {topic} failed (rc={result.rc})")
            self.logger.debug("sent text to {}", topic)

        for media_path in msg.media or []:
            built = await asyncio.to_thread(self._build_media_payload, msg, media_path, to_agent)
            if built is None:
                continue
            payload, kind = built
            result = self._client.publish(topic, payload, qos=1)
            if result.rc != 0:
                raise RuntimeError(f"MQTT publish of {kind} to {topic} failed (rc={result.rc})")
            self.logger.info("sent {} ({} B) to {}", kind, len(payload), topic)
