import time

from .payload import Payload
from .worker import Worker
from .features import is_available


if is_available("concurrency"):
    from gevent import monkey, pool


class Consumer(Worker):

    source_handlers = {
        "ready": "spawn_job"
    }
    outputs = ['results']

    def __init__(self, *args, **kwargs):
        super(Consumer, self).__init__(*args, **kwargs)

        self.pool = None

    def setup(self):
        super(Consumer, self).setup()

        if "concurrency" in self.config:
            monkey.patch_all(thread=False)
            self.pool = pool.Pool(self.config.concurrency)

    def spawn_job(self, payload):
        if self.pool is not None:
            self.pool.spawn(self.run_job, payload)
        else:
            self.run_job(payload)

    def run_job(self, payload):
        self.logger.debug("Job started", extra={"payload": payload})
        start_time = time.time()

        try:
            job = Payload.deserialize(payload)
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
        self.outputs['results'].put(
            {"payload": payload, "time": end_time - start_time}
        )
