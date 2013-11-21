import os
import time
import types


def get_script_content(command):
    current_path = os.path.dirname(__file__)

    content = ""
    with open("%s/lua/%s.lua" % (current_path, command)) as fd:
        content = fd.read()

    return content


def add_qadd(client):
    content = get_script_content("qadd")

    method = client.register_script(content)

    def qadd(self, queue, when, job_key, job_payload):
        return method(
            keys=[
                queue + ":scheduled",
                queue + ":ready",
                queue + ":working",
                queue + ":jobs:pool"
            ],
            args=[time.time(), when, job_key, job_payload],
            client=self
        )

    client.qadd = types.MethodType(qadd, client)


def add_qpop(client):
    content = get_script_content("qpop")

    method = client.register_script(content)

    def qpop(self, queue, cutoff, maxitems):
        return method(
            keys=[
                queue + ":scheduled",
                queue + ":working",
                queue + ":jobs:pool"
            ],
            args=[time.time(), cutoff, maxitems],
            client=self
        )

    client.qpop = types.MethodType(qpop, client)


def add_qfinish(client):
    content = get_script_content("qfinish")

    method = client.register_script(content)

    def qfinish(self, queue, *job_keys):
        args = [time.time()]
        args.extend(job_keys)
        return method(
            keys=[
                queue + ":working",
                queue + ":done",
                queue + ":jobs:pool"
            ],
            args=args,
            client=self
        )

    client.qfinish = types.MethodType(qfinish, client)


def extend_redis(client):
    add_qadd(client)
    add_qpop(client)
    add_qfinish(client)


__all__ = [extend_redis]
