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
            job = Job()
            job.deserialize(payload)
            self.greenlet_pool.spawn(self.run_job, job)
            self.capacity -= 1
            self.logger.debug("Queued job: %s", job)

    def run_job(self, job):
        self.logger.debug("Job started %s", job)
        self.redis.qworkon("rotterdam", job.unique_key)
        start_time = time.time()

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

        self.logger.debug(
            "Job completed in %0.2fs seconds", end_time - start_time
        )
        self.redis.qfinish("rotterdam", job.unique_key)
        self.capacity += 1
