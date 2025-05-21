from application.event_loop_runner import EventLoopRunner
from application.factory.random_event_factory import RandomEventFactory
from infrastructure.kafka_writer import KafkaWriter


class EventsGenerator:
    def __init__(self, config):
        self._topic = config.topic
        self._actions = config.actions
        self._products = config.products
        self._users = config.users

    def start(self):
        writer = KafkaWriter(
            bootstrap_servers=self._topic.BOOTSTRAP_SERVERS,
            topic_encode=self._topic.DEFAULT_ENCODE,
            topic_name=self._topic.TOPIC_NAME
        )
        factory = RandomEventFactory(self._users, self._products, self._actions)
        runner = EventLoopRunner(interval=self._topic.WRITE_INTERVAL, factory=factory, writer=writer)
        runner.start()
