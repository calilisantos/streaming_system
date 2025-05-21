from application.events_generator import EventsGenerator
from infrastructure.configs.app_config import AppConfig

if __name__ == "__main__":
    config = AppConfig()
    EventsGenerator(config).start()
