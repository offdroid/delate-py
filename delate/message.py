import json


class Message:
    """A universal wrapper around an api response message."""

    def __init__(self, message: str, store_raw: bool = False):
        msg_json = json.loads(message)

        for k in ["source", "timestamp", "content"]:
            if k not in msg_json:
                raise MessageInvalidError(
                    f"Missing essential key '{k}' in message: {msg_json}"
                )

        if store_raw:
            self.raw_str = message
        self.raw = msg_json

        self.content = msg_json["content"]
        self.timestamp = msg_json["timestamp"]
        self.source = msg_json["source"]


class MessageInvalidError(Exception):
    """Error if the message differs from the assertions when being parsed."""
