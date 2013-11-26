class RotterdamError(Exception):
    pass


class NoSuchJob(RotterdamError):
    pass


class ConnectionError(RotterdamError):
    pass


class InvalidPayload(RotterdamError):
    pass


class JobEnqueueError(RotterdamError):
    pass
