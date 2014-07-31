import Queue
import time

from .worker import Worker
from .payload import Payload


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

        self.multiplier = 1

        if "concurrency" in self.config:
            self.multiplier = self.config.concurrency

        self.capacity = self.config.num_consumers * self.multiplier

        self.logger.debug(
            "Arbitrating jobs for: %s", ",".join(self.config.queues)
        )

    def heartbeat(self):
        super(Arbiter, self).heartbeat()

        payloads = self.redis.qpop(
            self.config.queues,
            int(time.time()), self.capacity
        )

        for payload in payloads:
            try:
                self.outputs['ready'].put_nowait(payload)
                self.capacity -= 1
                self.logger.debug("Queued job", extra={"payload": payload})
            except Queue.Full:
                break

    def handle_finished_job(self, result):
        self.capacity += 1

        job = Payload.deserialize(result['payload'])
        self.redis.qfinish(job.queue_name, job.unique_key)

    def expand_capacity(self, *args):
        self.capacity += self.multiplier
        self.logger.debug("capacity set to %d", self.capacity)

    def contract_capacity(self, *args):
        self.capacity -= self.multiplier
        self.logger.debug("capacity set to %d", self.capacity)
