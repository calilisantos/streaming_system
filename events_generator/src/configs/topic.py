from dotenv import load_dotenv
from os import getenv


load_dotenv()


class Topic:
    BOOTSTRAP_SERVERS = "kafka-server:9092"
    DEFAULT_ENCODE = "utf-8"
    TOPIC_NAME = getenv("TOPIC_NAME")
    WRITE_INTERVAL = 5
