from .worker import Worker


class Injector(Worker):

    source_handlers = {
        "connection": "handle_incoming_jobs"
    }

    def handle_incoming_jobs(self):
        for job in self.sources['connection']:
            self.logger.debug("got job: %s (%s)", job, job.unique_key)

            self.redis.qadd(
                "rotterdam",
                job.when,
                job.unique_key,
                job.serialize()
            )
