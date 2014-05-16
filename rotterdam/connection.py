import errno
import json
import logging
import os
import socket

from .payload import Payload
from .exceptions import NoSuchJob, InvalidPayload


SOCKET_BUFFER_SIZE = 4096


class Connection(object):

    def __init__(self, host='', port=8765):
        self.host = host
        self.port = port

        self.socket = None

        self.logger = logging.getLogger(__name__)

    def open(self):
        existing_fd = None
        if "ROTTERDAM_SOCKET_FD" in os.environ:
            existing_fd = int(os.environ.pop("ROTTERDAM_SOCKET_FD"))
            self.socket = socket.fromfd(
                existing_fd,
                socket.AF_INET, socket.SOCK_STREAM
            )
        else:
            self.socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM
            )
        self.socket.setblocking(0)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if existing_fd is None:
            self.socket.bind((self.host, self.port))
        self.socket.listen(5)

    def close(self):
        self.socket.close()

    def __iter__(self):
        accepted = False

        while not accepted:
            try:
                conn, addr = self.socket.accept()
                accepted = True
            except socket.error as e:
                if e.errno == errno.EAGAIN:
                    accepted = False

        self.logger.debug("connection from %s:%s", addr[0], addr[1])

        message = ''

        while True:
            try:
                chunk = conn.recv(SOCKET_BUFFER_SIZE)
                if chunk:
                    message = message + chunk
            except socket.error as e:
                if e.errno not in [errno.EAGAIN, errno.EINTR]:
                    raise

            if not message:
                break

            if "\n" in message:
                message, extra = message.split("\n", 1)

            try:
                job = Payload.deserialize(message)
            except InvalidPayload:
                response = {"status": "error", "message": "invalid payload"}
            except NoSuchJob:
                response = {"status": "error", "message": "no such job"}
            except Exception as e:
                self.logger.exception("Unhandled exception when loading job")
                response = {"status": "error", "message": str(e)}
            else:
                response = {"status": "ok"}

            conn.sendall(json.dumps(response) + "\n")

            if response['status'] == "ok":
                yield job

            message = extra

        conn.close()
