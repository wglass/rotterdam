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
        'results': 'handle_finished_job'
    }
    outputs = ['ready']

    def setup(self):
        super(Arbiter, self).setup()
        self.capacity = (
            self.config.num_unloaders * self.config.greenlet_pool_size
        )

    def heartbeat(self):
        super(Arbiter, self).heartbeat()

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

    def handle_finished_job(self, result):
        self.capacity += 1

    def expand_capacity(self, *args):
        self.capacity += self.config.greenlet_pool_size
        self.logger.debug("capacity set to %d", self.capacity)

    def contract_capacity(self, *args):
        self.capacity -= self.config.greenlet_pool_size
        self.logger.debug("capacity set to %d", self.capacity)
