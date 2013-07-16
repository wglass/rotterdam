import hashlib
import json
import socket
import time
import types


class Client(object):

    def __init__(self, host, port=8765):
        self.host = host
        self.port = port

    def enqueue(self, func, *args, **kwargs):
        if isinstance(func, basestring):
            module, function = func.split(":")
        elif isinstance(func, types.FunctionType):
            module = func.__module__,
            function = func.__name__

        uniqueness = hashlib.md5()

        uniqueness.update(str(module))
        uniqueness.update(str(function))

        for arg in args:
            uniqueness.update(str(arg))
        for arg_name in sorted(kwargs.keys()):
            uniqueness.update(
                str(arg_name) + "=" + str(kwargs[arg_name])
            )

        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.connect((self.host, self.port))

        connection.sendall(json.dumps({
            "when": int(time.time()),
            "unique_key": uniqueness.hexdigest(),
            "module": module,
            "function": function,
            "args": args,
            "kwargs": kwargs
        }) + "\n")

        connection.close()
