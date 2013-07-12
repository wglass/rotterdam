import errno
import socket

from .job import Job


SOCKET_BUFFER_SIZE = 4096


class Connection(object):

    def __init__(self, host='', port=8765):
        self.host = host
        self.port = port

        self.socket = None

    def open(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setblocking(0)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))
        self.socket.listen(5)

    def close(self):
        self.socket.close()

    def iterjobs(self):
        conn, addr = self.socket.accept()

        message = ''

        while True:
            try:
                chunk = self.socket.recv(SOCKET_BUFFER_SIZE)
                if chunk:
                    message = message + chunk
            except socket.error as e:
                if e.errno not in [errno.EAGAIN, errno.EINTR]:
                    raise

            if not message:
                break

            if "\n" in message:
                message, extra = message.split("\n", 1)

            job = Job()
            job.deserialize(message)

            yield job

            message = extra

        conn.close()
