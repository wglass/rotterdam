import errno
import logging
import Queue
import select
import time

import redis

from .job import Job
from .exceptions import NoSuchJob
from .redis_extensions import extend_redis


class Arbiter(object):

    def __init__(
            self,
            config,
            connection,
            ready_queue, taken_queue, results_queue
    ):
        self.config = config
        self.connection = connection

        self.ready_queue = ready_queue
        self.taken_queue = taken_queue
        self.results_queue = results_queue

        self.logger = logging.getLogger(__name__)

        self.redis = None

    def setup(self):

        if ":" in self.config.redis_host:
            host, port = self.config.redis_host.split(":")
            self.redis = redis.Redis(host=host, port=port)
        else:
            self.redis = redis.Redis(host=self.config.redis_host)

        extend_redis(self.redis)

        self.fill_ready_queue()

    def arbitrate(self, timeout):
        try:
            (sources_with_data, [], []) = select.select(
                [
                    self.connection.socket,
                    self.taken_queue._reader,
                    self.results_queue._reader
                ],
                [], [],
                timeout
            )

            while len(sources_with_data) > 0:
                source_with_data = sources_with_data.pop(0)

                if source_with_data == self.connection.socket:
                    self.handle_incoming_job()
                elif source_with_data == self.taken_queue._reader:
                    self.handle_taken_job()
                elif source_with_data == self.results_queue._reader:
                    self.handle_finished_job()

        except select.error as e:
            if e.args[0] not in [errno.EAGAIN, errno.EINTR]:
                raise
        except OSError as e:
            if e.errno not in [errno.EAGAIN, errno.EINTR]:
                raise
        except IOError as e:
            if e.errno not in [errno.EAGAIN, errno.EINTR, errno.EBADF]:
                raise

    def fill_ready_queue(self):
        while True:
            try:
                payloads = self.redis.qget("rotterdam", int(time.time()))

                if not payloads:
                    break

                for payload in payloads:
                    job = Job()
                    job.deserialize(payload)
                    self.logger.debug(
                        "Queueing job: %s", job
                    )
                    self.redis.qsetstate(
                        "rotterdam",
                        "schedule",
                        "ready",
                        job.unique_key
                    )
                    self.ready_queue.put_nowait(job)
            except Queue.Full:
                break

    def handle_incoming_job(self):
        for job in self.connection.iterjobs():
            self.logger.debug("got job: %s", job)
            try:
                job.load()
            except NoSuchJob:
                continue

            self.redis.qadd(
                "rotterdam",
                job.when,
                job.unique_key,
                job.serialize()
            )

        self.fill_ready_queue()

    def handle_taken_job(self):
        while True:
            try:
                taken = self.taken_queue.get_nowait()

                self.logger.debug("Job started %s", taken["job"])

                self.redis.qsetstate(
                    "rotterdam",
                    "ready",
                    "working",
                    taken["job"].unique_key
                )

            except Queue.Empty:
                break

        self.fill_ready_queue()

    def handle_finished_job(self):
        while True:
            try:
                result = self.results_queue.get_nowait()

                self.logger.debug(
                    "Job completed in %0.2fs seconds",
                    result["time"]
                )

                self.redis.qsetstate(
                    "rotterdam",
                    "working",
                    "done",
                    result["job"].unique_key
                )

            except Queue.Empty:
                break
