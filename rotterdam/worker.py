import errno
import os
import Queue
import select
import sys

from .proc import Proc


class Worker(Proc):

    signal_map = {
        "tstp": "toggle_active",
        "term": "wind_down_gracefully",
        "quit": "wind_down_immediately"
    }
    source_handlers = {}
    outputs = {}

    def __init__(self, master):
        super(Worker, self).__init__()

        self.config = master.config
        self.redis = master.redis

        possible_channels = {
            "ready": master.ready_queue,
            "results": master.results_queue,
            "connection": master.conn
        }
        self.sources = {
            channel_name: possible_channels[channel_name]
            for channel_name in self.source_handlers.keys()
            if channel_name in possible_channels
        }
        self.handlers = {
            input_name: getattr(self, handler_name)
            for input_name, handler_name in self.source_handlers.iteritems()
            if getattr(self, handler_name, None)
        }
        self.outputs = {
            channel_name: possible_channels[channel_name]
            for channel_name in self.outputs
            if channel_name in possible_channels
        }

        self.age = 0

        self.alive = True
        self.active = True

    def run(self):
        super(Worker, self).run()

        while self.alive:
            try:
                (sources_with_data, [], []) = select.select(
                    [
                        selectable_of(source)
                        for source in self.sources.values()
                    ], [], [],
                    self.config.heartbeat_interval
                )

                while self.active and len(sources_with_data) > 0:
                    source_with_data = sources_with_data.pop(0)

                    for source_name, input_source in self.sources.iteritems():
                        if source_with_data == selectable_of(input_source):
                            if getattr(input_source, "get_nowait", None):
                                try:
                                    payload = input_source.get_nowait()
                                except Queue.Empty:
                                    continue
                                self.handlers[source_name](payload)
                            else:
                                self.handlers[source_name]()

                self.heartbeat()
                self.age += 1

            except select.error as e:
                if e.args[0] not in [errno.EAGAIN, errno.EINTR]:
                    raise
            except OSError as e:
                if e.errno not in [errno.EAGAIN, errno.EINTR]:
                    raise
            except IOError as e:
                if e.errno not in [errno.EAGAIN, errno.EINTR, errno.EBADF]:
                    raise
            except KeyboardInterrupt:
                self.wind_down_gracefully()
            except Exception:
                self.logger.exception("Unhandled error during run loop")
                sys.exit(-1)

    def toggle_active(self, *args):
        self.active = not self.active

    def wind_down_gracefully(self, *args):
        self.alive = False
        self.active = False

    def wind_down_immediately(self, *args):
        self.alive = False
        self.active = False
        sys.exit(0)

    def heartbeat(self):
        if self.pid == os.getpid() and self.parent_pid != os.getppid():
            self.logger.info("Parent process changed! winding down.")
            self.alive = False


def selectable_of(input_source):
    if hasattr(input_source, "_reader"):
        return input_source._reader
    elif hasattr(input_source, "socket"):
        return input_source.socket
    else:
        return input_source
