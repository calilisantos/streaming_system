from configs.logs import Logs
from application.events_generator import EventsGenerator
from infrastructure.configs.app_config import AppConfig

if __name__ == "__main__":
    Logs.configure()
    config = AppConfig()
    EventsGenerator(config).start()
