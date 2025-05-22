import logging
from time import sleep


class EventLoopRunner:
    def __init__(self, factory, interval, writer):
        self._factory = factory
        self._interval = interval
        self._writer = writer
        self._logger = logging.getLogger(__name__)

    def start(self):
        self._logger.info('Starting the events scan')
        try:
            while True:
                self._logger.info('Creating event')
                event = self._factory.create_event()
                self._logger.info('Writing event')
                self._writer.send_event(event.to_dict())
                self._logger.info('Waiting the next scan')
                sleep(self._interval)
        except Exception:
            self._logger.exception('An error occurred during event scan loop.')
        finally:
            self._logger.warning('Closing the events scan')
            self._writer.close()
