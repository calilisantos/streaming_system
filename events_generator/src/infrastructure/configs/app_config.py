from configs.topic import Topic
from configs.entities import Users, Products, Actions


class AppConfig:
    def __init__(self):
        self._topic = Topic()
        self._users = Users()
        self._products = Products()
        self._actions = Actions()

    @property
    def topic(self):
        return self._topic

    @property
    def users(self):
        return self._users

    @property
    def products(self):
        return self._products

    @property
    def actions(self):
        return self._actions
