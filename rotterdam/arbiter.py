import Queue
import time

from .child import Child
from .job import Job


class Arbiter(Child):

    source_handlers = {
        'taken': 'handle_taken_job',
        'results': 'handle_finished_job'
    }

    def setup(self):
        super(Arbiter, self).setup()

        self.fill_ready_queue()

    def heartbeat(self):
        super(Arbiter, self).heartbeat()
        self.fill_ready_queue()

    def fill_ready_queue(self):
        while True:
            try:
                payloads = self.redis.qpop("rotterdam", int(time.time()))

                if not payloads:
                    break

                for payload in payloads:
                    job = Job()
                    job.deserialize(payload)
                    self.logger.debug("Queueing job: %s", job)
                    self.outputs['ready'].put_nowait(job)
            except Queue.Full:
                break

    def handle_taken_job(self, taken):
        self.logger.debug("Job started %s", taken["job"])
        self.redis.qworkon("rotterdam", taken["job"].unique_key)

    def handle_finished_job(self, result):
        self.logger.debug("Job completed in %0.2fs seconds", result["time"])
        self.redis.qfinish("rotterdam", result["job"].unique_key)
