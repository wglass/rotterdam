import redis

from .child import Child
from .exceptions import NoSuchJob
from .redis_extensions import extend_redis


class Injector(Child):

    source_handlers = {
        "connection": "handle_incoming_jobs"
    }

    def __init__(self, *args, **kwargs):
        super(Injector, self).__init__(*args, **kwargs)
        self.redis = None

    def setup(self):
        if ":" in self.config.redis_host:
            host, port = self.config.redis_host.split(":")
            self.redis = redis.Redis(host=host, port=port)
        else:
            self.redis = redis.Redis(host=self.config.redis_host)

        extend_redis(self.redis)

        super(Injector, self).setup()

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
