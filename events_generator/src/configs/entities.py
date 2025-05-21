from abc import ABC, abstractmethod


class AbstractEntity(ABC):
    @property
    @abstractmethod
    def values(self):
        pass

    @property
    def length(self) -> int:
        return len(self.values)


class Actions(AbstractEntity):
    ACTIONS_NAMES = ["login", "click", "purchase"]

    @property
    def values(self):
        return self.ACTIONS_NAMES


class Products(AbstractEntity):
    PRODUCTS_IDS = [123, 456, 789]

    @property
    def values(self):
        return self.PRODUCTS_IDS


class Users(AbstractEntity):
    USERS_IDS = [234, 5678, 208, 619]

    @property
    def values(self):
        return self.USERS_IDS
