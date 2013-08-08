import errno
import logging
import os
import Queue
import signal
import sys
import time

from gevent import pool, monkey, select


class Worker(object):

    understood_signals = ["QUIT", "TERM", "INT"]

    def __init__(self, in_queue, taken_queue, out_queue, config):
        self.in_queue = in_queue
        self.taken_queue = taken_queue
        self.out_queue = out_queue
        self.config = config

        self.alive = True
        self.active = True
        self.age = 0

        self.smtp_pool = None

        self.logger = logging.getLogger(__name__)

    def setup(self):
        self.pid = os.getpid()
        self.parent_pid = os.getppid()
        self.setup_signals()

    def setup_signals(self):

        for signal_name in self.understood_signals:
            signal.signal(
                getattr(signal, "SIG%s" % signal_name),
                getattr(self, "handle_%s" % signal_name.lower())
            )

    def run(self):
        monkey.patch_all(thread=False)

        greenlet_pool = pool.Pool(self.config.greenlet_pool_size)

        while self.alive:
            try:
                ready = select.select(
                    [self.in_queue._reader],
                    [], [],
                    self.config.heartbeat_interval
                )
                if not ready[0] or not self.active:
                    self.heartbeat()

                self.age += 1
                job = self.in_queue.get_nowait()
                greenlet_pool.spawn(
                    self.handle_job, job
                )

            except Queue.Empty:
                continue
            except select.error as error:
                if error.args[0] not in [errno.EAGAIN, errno.EINTR]:
                    raise
            except OSError as e:
                if e.errno not in [errno.EAGAIN, errno.EINTR]:
                    raise
            except SystemExit:
                raise
            except Exception as e:
                self.logger.exception(e)
                sys.exit(-1)

        greenlet_pool.join(raise_error=True)

    def heartbeat(self):
        if self.pid == os.getpid() and self.parent_pid != os.getppid():
            self.logger.info("Parent process changed! shutting down.")
            self.alive = False

    def handle_job(self, job):
        start_time = time.time()
        self.taken_queue.put(
            {
                "time": start_time,
                "job": job
            }
        )

        try:
            job.load()
        except:
            self.logger.exception("Exception when loading job!")
        else:
            try:
                job.run()
            except:
                self.logger.exception("Exception when running job!")

        end_time = time.time()

        self.out_queue.put(
            {
                "time": end_time - start_time,
                "job": job
            }
        )

    def handle_int(self, signal, frame):
        self.active = not self.active

    def handle_quit(self, signal, frame):
        self.active = False
        self.alive = False

    def handle_term(self, signal, frame):
        self.active = False
        self.alive = False
        sys.exit(0)
