import logging
import os
import signal

from setproctitle import setproctitle


class Proc(object):

    signal_map = {}

    def __init__(self):
        self.logger = logging.getLogger(self.__module__)

        self.pid = None
        self.parent_pid = None

    @property
    def name(self):
        return self.__class__.__name__.lower()

    def setup(self):
        self.pid = os.getpid()
        self.parent_pid = os.getppid()
        self.setup_signals()
        setproctitle("rotterdam: %s" % self.name)

    def run(self):
        self.setup()
        self.logger.info("Starting %s (%d)", self.name, int(self.pid))

    def setup_signals(self):
        for signal_name, handler_name in self.signal_map.iteritems():
            signal.signal(
                getattr(signal, "SIG%s" % signal_name.upper()),
                getattr(self, handler_name)
            )
