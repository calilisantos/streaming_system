from datetime import datetime


class Event:
    def __init__(self, user_id, product_id, action_name):
        self.user_id = user_id
        self.product_id = product_id
        self.action_name = action_name
        self.timestamp = datetime.now().isoformat()

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "event_type": self.action_name,
            "timestamp": self.timestamp,
            "game_id": self.product_id,
            "payload": {
                "button_id": "789"
            } if self.action_name == "click" else {}
        }
