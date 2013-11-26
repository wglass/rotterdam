import time

from .worker import Worker
from .payload import Payload


class Consumer(Worker):

    def setup(self):
        super(Consumer, self).setup()
        self.capacity = self.config.greenlet_pool_size
        self.queues = self.config.queues.split(",")
        self.logger.debug("Consuming jobs for: %s", ",".join(self.queues))

    def heartbeat(self):
        super(Consumer, self).heartbeat()

        payloads = self.redis.qpop(
            self.queues,
            int(time.time()), self.capacity
        )

        for payload in payloads:
            try:
                job = Payload(payload)
            except:
                self.logger.exception("Error when loading job")
                continue
            self.greenlet_pool.spawn(self.run_job, job)
            self.capacity -= 1
            self.logger.debug("job queued: %s", job)

    def run_job(self, job):
        self.logger.debug("job started: %s", job)
        start_time = time.time()

        try:
            job.run()
        except:
            self.logger.exception("Exception when running job!")

        end_time = time.time()

        self.logger.debug(
            "Job completed in %0.2fs seconds", end_time - start_time
        )
        self.redis.qfinish(job.queue, job.unique_key)
        self.capacity += 1
