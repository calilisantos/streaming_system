import logging


class Logs:
    LOG_DATE_FORMAT: str = '%Y-%m-%d %H:%M:%S'
    LOG_FORMAT: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOG_STYLE: str = '%'

    @staticmethod
    def configure():
        logging.basicConfig(
            level=logging.INFO,
            format=Logs.LOG_FORMAT,
            datefmt=Logs.LOG_DATE_FORMAT,
            style=Logs.LOG_STYLE,
            handlers=[
                logging.FileHandler('/app/logs/events_generator.log'),
                logging.StreamHandler()
            ]
        )
