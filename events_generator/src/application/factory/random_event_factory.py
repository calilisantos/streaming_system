from domain.event import Event
from numpy.random import randint


class RandomEventFactory:
    def __init__(self, users, products, actions):
        self._users = users
        self._products = products
        self._actions = actions

    def _get_random(self, entity):
        return entity.values[randint(0, entity.length)]

    def create_event(self):
        return Event(
            user_id=self._get_random(self._users),
            product_id=self._get_random(self._products),
            action_name=self._get_random(self._actions)
        )
