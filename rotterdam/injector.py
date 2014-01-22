from .worker import Worker


class Injector(Worker):

    source_handlers = {
        "connection": "handle_incoming_jobs"
    }

    def handle_incoming_jobs(self):
        for job in self.sources['connection']:
            self.logger.debug("job recieved: %s", job)

            self.redis.qadd(
                job.queue_name,
                job.when,
                job.unique_key,
                job.serialize()
            )
