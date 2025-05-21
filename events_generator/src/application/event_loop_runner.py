from time import sleep


class EventLoopRunner:
    def __init__(self, factory, interval, writer):
        self._factory = factory
        self._interval = interval
        self._writer = writer

    def start(self):
        try:
            while True:
                event = self._factory.create_event()
                self._writer.send_event(event.to_dict())
                sleep(self._interval)
        finally:
            self._writer.close()
