import time

from .payload import Payload
from .worker import Worker


class Consumer(Worker):

    source_handlers = {
        "ready": "run_job"
    }
    outputs = ['results']

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
