"""MQTT management contract."""

from nanobot.channels._manifest import field, required_fields
from nanobot.channels.contracts import ChannelSetupSpec
from nanobot.channels.plugin import ChannelPlugin

SETUP_SPEC = ChannelSetupSpec(
    fields={
        "brokerHost": field(default="127.0.0.1"),
        "brokerPort": field("int", default=1883),
        "agentId": field(),
        "username": field(),
        "password": field("secret"),
        "transport": field("enum", choices=("tcp", "ws"), default="tcp"),
        "tls": field("bool", default=False),
        "topicPrefix": field(default="im"),
        "countersign": field("secret"),
        "displayName": field(),
        "allowFrom": field("list"),
    },
    required=required_fields("brokerHost", "agentId"),
    official_url="https://mosquitto.org/",
)

PLUGIN = ChannelPlugin(
    name="mqtt",
    display_name="MQTT",
    runtime=f"{__package__}.runtime:MQTTChannel",
    setup=SETUP_SPEC,
    dependencies=("paho-mqtt>=2.1.0",),
)
