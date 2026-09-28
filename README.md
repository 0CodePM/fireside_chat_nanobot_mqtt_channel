# nanobot MQTT Channel (Fireside)

A [nanobot](https://github.com/HKUDS/nanobot) channel plugin that connects a nanobot
agent to a mosquitto broker and speaks the **Fireside (FireChat)** IM protocol, so the
agent can chat with humans and other agents over MQTT.

The agent joins the mesh as `ag_<agentId>`: it subscribes to its own inbox, answers
messages from humans and agents, and can send text, images, and files back.

## Files

| File | Purpose |
| --- | --- |
| `__init__.py` | Package marker |
| `manifest.py` | Channel setup spec (config fields, defaults, dependency) |
| `runtime.py` | `MQTTChannel` implementation (paho-mqtt, payload codec, media) |
| `mqtt-config.json` | Example `channels.mqtt` config (placeholders — fill in your own values) |

## Requirements

- nanobot (HKUDS/nanobot)
- `paho-mqtt>=2.1.0` (declared in the manifest; install with `pip install 'paho-mqtt>=2.1.0'`)
- A mosquitto broker (or any MQTT 3.1.1 broker) with per-client credentials

If `paho-mqtt` is missing the channel degrades gracefully: it logs an error and stays
inactive instead of crashing the gateway.

## Installation

Copy this folder into the nanobot package as a channel:

```
nanobot/channels/mqtt/
├── __init__.py
├── manifest.py
└── runtime.py
```

Then restart the nanobot gateway (channels are discovered at startup).

## Configuration

Add a `channels.mqtt` block to your nanobot `config.json` (camelCase keys):

```json
{
  "enabled": true,
  "brokerHost": "your-broker-host",
  "brokerPort": 1883,
  "agentId": "ag_your_agent",
  "username": "ag_your_agent",
  "password": "REPLACE_WITH_YOUR_AGENT_PASSWORD",
  "transport": "tcp",
  "topicPrefix": "im",
  "allowFrom": ["*"],
  "countersign": "REPLACE_WITH_YOUR_COUNTERSIGN",
  "displayName": "Your Agent Name"
}
```

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Enable the channel |
| `brokerHost` | str | `127.0.0.1` | **Required.** Broker address |
| `brokerPort` | int | `1883` | Broker port |
| `agentId` | str | — | **Required.** Agent identity; the MQTT-visible name is always `ag_<agentId>` (prefix auto-added if missing) |
| `username` | str | — | Broker auth account; leave empty for anonymous brokers |
| `password` | secret | — | Broker password for `username` |
| `transport` | `tcp` \| `ws` | `tcp` | `ws` = MQTT over WebSocket |
| `tls` | bool | `false` | Enable TLS (required for public endpoints, e.g. port 8883) |
| `topicPrefix` | str | `im` | Inbox namespace |
| `countersign` | secret | — | 16-char agent countersign; attached to outbound agent-directed messages |
| `displayName` | str | `ag_<agentId>` | Sender display name on outbound payloads |
| `allowFrom` | list | `[]` | Sender ids allowed to talk to the agent; `["*"]` allows everyone, empty denies all |

## Topics

- **Subscribe:** `{topicPrefix}/ag_<agentId>/inbox` (QoS 1)
- **Publish:** `{topicPrefix}/<target>/inbox` (QoS 1)
  - Replies to agents (`ag_*` targets) carry the agent's `countersign`
  - Replies to humans never carry it

Countersign verification is the receiving agent's responsibility — it is a
payload-level convention; the broker does not inspect payloads.

## Payload format

Matches the Fireside webim envelope.

**Text (JSON):**

```json
{
  "from": "ag_your_agent",
  "display_name": "Your Agent Name",
  "msg_type": "im",
  "content_type": "text",
  "text": "hello",
  "ts": 1759000000000
}
```

**Images / files:**

- Inline JSON: `content_type: "image"` + base64 `image_data` (images under 70 KB)
- Binary frame v1: `1B ver | 1B type | 2B meta_len (BE) | meta JSON | data`
  - `0x01` TEXT, `0x02` IMAGE (≤ 2 MB), `0x03` FILE (≤ 10 MB), `0x04` SYSTEM
- Detection: first byte `0x01` → binary frame, `0x7B` (`{`) → JSON, else raw text

Received media is saved under the `media/mqtt` runtime directory and passed to the
agent loop via `InboundMessage.media`.

## Behavior

- paho-mqtt runs in a background thread with auto-reconnect, so the connection stays
  alive long-term without blocking the agent's main loop
- Own echoes (messages where `from` equals the agent's identity) are ignored
- Inbound messages are routed to the agent loop as DMs keyed by sender id
