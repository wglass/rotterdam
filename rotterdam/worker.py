import sys
import time

from .child import Child


class Worker(Child):

    signal_map = {
        "int": "toggle_active"
    }
    source_handlers = {
        "ready": "run_job"
    }

    age = 0

    def setup(self):
        super(Worker, self).setup()
        self.age = 0

    def run_job(self, job):
        self.age += 1

        start_time = time.time()
        self.outputs['taken'].put(
            {"job": job, "time": start_time}
        )

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

    def toggle_active(self, signal, frame):
        self.active = not self.active

    def wind_down_gracefully(self, signal, frame):
        self.active = False
        self.alive = False

    def wind_down_immediately(self, signal, frame):
        self.active = False
        self.alive = False
        sys.exit(0)
