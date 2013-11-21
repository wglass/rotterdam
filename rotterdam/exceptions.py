class RotterdamError(Exception):
    pass


class NoSuchJob(RotterdamError):
    pass


class ConnectionError(RotterdamError):
    pass


class InvalidJobPayload(RotterdamError):
    pass


class JobEnqueueError(RotterdamError):
    pass
