from kafka import KafkaProducer
from json import dumps


class KafkaWriter:
    def __init__(self, bootstrap_servers, topic_encode, topic_name):
        self._topic_name = topic_name
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: dumps(x).encode(topic_encode)
        )

    def send_event(self, event):
        self._producer.send(topic=self._topic_name, value=event)

    def close(self):
        self._producer.flush()
        self._producer.close()
