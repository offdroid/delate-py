import json


class Message:
    """ A universal wrapper around an api response message """

    def __init__(self, message: str):
        msg_json = json.loads(message)

        for k in ["source", "timestamp", "content"]:
            if k not in msg_json:
                raise MessageInvalidError(f"Missing key '{k}' in message: {msg_json}")

        self.raw_str = message
        self.raw = msg_json
        self.raw_content = msg_json["content"]

        self.timestamp = msg_json["timestamp"]
        self.source = msg_json["source"]


class MessageInvalidError(Exception):
    """ Error if the message differs from the assertions when being parsed """
