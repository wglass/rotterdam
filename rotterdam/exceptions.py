class RotterdamError(Exception):
    pass


class NoSuchJob(RotterdamError):
    pass


class ConnectionError(RotterdamError):
    pass
