import errno
import socket

from .job import Job

SOCKET_BUFFER_SIZE = 4096


class JobIterator(object):

    def __init__(self, connection):
        self.connection = connection

    def __iter__(self):
        return self

    def next(self):
        message = ''
        while True:
            try:
                chunk = self.connection.recv(SOCKET_BUFFER_SIZE)
                if not chunk:
                    break
                message = message + chunk
            except socket.error as e:
                if e.errno not in [errno.EAGAIN, errno.EINTR]:
                    raise

        job = Job()

        job.deserialize(message)

        if not job:
            raise StopIteration

        return job
