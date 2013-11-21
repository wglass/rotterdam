import time

from .worker import Worker


class Unloader(Worker):

    source_handlers = {
        "ready": "run_job"
    }

    age = 0

    def run_job(self, job):
        self.age += 1

        start_time = time.time()
        self.outputs['taken'].put({"job": job, "time": start_time})

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

        self.outputs['results'].put(
            {"job": job, "time": end_time - start_time}
        )
