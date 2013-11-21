from .worker import Worker
from .exceptions import NoSuchJob


class Injector(Worker):

    source_handlers = {
        "connection": "handle_incoming_jobs"
    }

    def handle_incoming_jobs(self):
        for job in self.sources['connection'].iterjobs():
            self.logger.debug("got job: %s", job)
            try:
                job.load()
            except NoSuchJob:
                continue

            self.redis.qadd(
                "rotterdam",
                job.when,
                job.unique_key,
                job.serialize()
            )
