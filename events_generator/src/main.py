from configs.entities import Actions, Products, Users
from configs.topic import Topic
from events import Events
from json import dumps
from kafka import KafkaProducer
from numpy.random import randint
from time import sleep


class Write:
    def __init__(self):
        self._actions = Actions()
        self._current_action = None
        self._producer = None
        self._products = Products()
        self._topic = Topic
        self._users = Users()

    def _set_producer(self):
        self._producer = KafkaProducer(
            bootstrap_servers=self._topic.BOOTSTRAP_SERVERS,
            value_serializer=lambda x: dumps(x).encode("utf-8")
        )

    def _get_random_entity(self, entity):
        return entity.values[randint(0, entity.length)]

    def _write_event(self, event):
        self._producer.send(topic=self._topic.TOPIC_NAME, value=event)

    def run(self):
        while True:
            self._set_producer()
            current_user = self._get_random_entity(self._users)
            current_product = self._get_random_entity(self._products)
            current_action = self._get_random_entity(self._actions)
            event = Events(
                action_name=current_action,
                user_id=current_user,
                product_id=current_product
            ).create_events()
            self._write_event(event)
            sleep(self._topic.WRITE_INTERVAL)


Write().run()
