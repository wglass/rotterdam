import errno
import os
import Queue
import sys

from gevent import pool, monkey, select

from .proc import Proc


class Worker(Proc):

    default_signal_map = {
        "int": "toggle_active",
        "quit": "wind_down_gracefully",
        "term": "wind_down_immediately"
    }
    source_handlers = {}

    def __init__(self, boss):
        super(Worker, self).__init__()

        self.config = boss.config
        self.redis = boss.redis

        possible_channels = {
            "connection": boss.connection,
            "ready": boss.ready_queue,
            "taken": boss.taken_queue,
            "results": boss.results_queue
        }
        self.sources = {
            channel_name: possible_channels[channel_name]
            for channel_name in self.source_handlers.keys()
        }
        self.outputs = {
            channel_name: possible_channels[channel_name]
            for channel_name in self.outputs
        }
        self.signal_map = dict(
            self.default_signal_map.items() + self.signal_map.items()
        )
        self.handlers = {
            input_name: getattr(self, handler_name)
            for input_name, handler_name in self.source_handlers.iteritems()
        }

        self.greenlet_pool = None

        self.age = 0

        self.alive = True
        self.active = True

    def setup(self):
        super(Worker, self).setup()

        monkey.patch_all(thread=False)
        self.greenlet_pool = pool.Pool(self.config.greenlet_pool_size)

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
                            if hasattr(input_source, "get_nowait"):
                                try:
                                    payload = input_source.get_nowait()
                                except Queue.Empty:
                                    continue
                                self.greenlet_pool.spawn(
                                    self.handlers[source_name], payload
                                )
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
            except Exception:
                self.logger.exception("Unhandled error during run loop")
                sys.exit(-1)

        self.greenlet_pool.join(raise_error=True)

    def toggle_active(self, signal, frame):
        self.active = not self.active

    def wind_down_gracefully(self, signal, frame):
        self.alive = False
        self.active = False

    def wind_down_immediately(self, signal, frame):
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
