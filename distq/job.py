import errno
import socket
import struct


class Job(object):

    pack_format = "s"

    def __init__(self, connection):
        self.connection = connection
        pass

    @property
    def size(self):
        return struct.calcsize(self.pack_format)

    def __iter__(self):
        return self

    def next(self):
        message = ''
        while len(message) < self.size:
            try:
                chunk = self.connection.recv(self.size - len(message))
                if not chunk:
                    raise StopIteration
                message = message + chunk
            except socket.error as e:
                if e.errno not in [errno.EAGAIN, errno.EINTR]:
                    raise

        payload = struct.unpack(self.pack_format, message)

        if not payload:
            raise StopIteration

        return payload

    def send(self, message_tuple):
        payload = struct.pack(self.pack_format, *message_tuple)
        self.connection.sendall(payload)
