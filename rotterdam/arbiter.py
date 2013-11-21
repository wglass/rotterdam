import Queue
import time

from .worker import Worker
from .job import Job


class Arbiter(Worker):

    signal_map = {
        "ttin": "expand_capacity",
        "ttou": "contract_capacity"
    }
    source_handlers = {
        'taken': 'handle_taken_job',
        'results': 'handle_finished_job'
    }

    @classmethod
    def onboard(cls, boss):
        return cls(
            boss.config.master,
            boss.redis,
            sources={
                "taken": boss.taken_queue,
                "results": boss.results_queue
            },
            outputs={
                "ready": boss.ready_queue
            }
        )

    def setup(self):
        super(Arbiter, self).setup()
        self.capacity = (
            self.config.num_unloaders * self.config.greenlet_pool_size
        )

        self.fill_ready_queue()

    def heartbeat(self):
        super(Arbiter, self).heartbeat()
        self.fill_ready_queue()

    def fill_ready_queue(self):
            payloads = self.redis.qpop(
                "rotterdam",
                int(time.time()), self.capacity
            )

            for payload in payloads:
                job = Job()
                job.deserialize(payload)
                try:
                    self.outputs['ready'].put_nowait(job)
                    self.capacity -= 1
                    self.logger.debug("Queued job: %s", job)
                except Queue.Full:
                    break

    def handle_taken_job(self, taken):
        self.logger.debug("Job started %s", taken["job"])
        self.redis.qworkon("rotterdam", taken["job"].unique_key)

    def handle_finished_job(self, result):
        self.capacity += 1
        self.logger.debug("Job completed in %0.2fs seconds", result["time"])
        self.redis.qfinish("rotterdam", result["job"].unique_key)

    def expand_capacity(self, *args):
        self.capacity += self.config.greenlet_pool_size
        self.logger.debug("capacity set to %d", self.capacity)

    def contract_capacity(self, *args):
        self.capacity -= self.config.greenlet_pool_size
        self.logger.debug("capacity set to %d", self.capacity)
