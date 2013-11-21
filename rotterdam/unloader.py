import time

from .worker import Worker


class Unloader(Worker):

    source_handlers = {
        "ready": "run_job"
    }
    outputs = ['taken', 'results']

    def run_job(self, job):
        self.logger.debug("Job started %s", job)
        self.redis.qworkon("rotterdam", job.unique_key)
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
        self.logger.debug(
            "Job completed in %0.2fs seconds", end_time - start_time
        )
        self.redis.qfinish("rotterdam", job.unique_key)
