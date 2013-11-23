import time

from .worker import Worker
from .job import Job


class Consumer(Worker):

    def setup(self):
        super(Consumer, self).setup()
        self.capacity = self.config.greenlet_pool_size

    def heartbeat(self):
        super(Consumer, self).heartbeat()

        payloads = self.redis.qpop(
            "rotterdam",
            int(time.time()), self.capacity
        )

        for payload in payloads:
            try:
                job = Job(payload)
            except:
                self.logger.exception("Error when loading job")
                continue
            self.greenlet_pool.spawn(self.run_job, job)
            self.capacity -= 1
            self.logger.debug("Queued job: %s", job)

    def run_job(self, job):
        self.logger.debug("Job started %s", job)
        start_time = time.time()

        try:
            job.run()
        except:
            self.logger.exception("Exception when running job!")

        end_time = time.time()

        self.logger.debug(
            "Job completed in %0.2fs seconds", end_time - start_time
        )
        self.redis.qfinish("rotterdam", job.unique_key)
        self.capacity += 1
