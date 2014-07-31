import itertools
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
                "rotterdam:" + queue + ":scheduled",
                "rotterdam:" + queue + ":ready",
                "rotterdam:" + queue + ":working",
                "rotterdam:" + queue + ":jobs:pool"
            ],
            args=[time.time(), when, job_key, job_payload],
            client=self
        )

    client.qadd = types.MethodType(qadd, client)


def add_qpop(client):
    content = get_script_content("qpop")

    method = client.register_script(content)

    def qpop(self, queues, cutoff, maxitems):

        return method(
            keys=list(itertools.chain.from_iterable(
                [
                    "rotterdam:" + queue + ":scheduled",
                    "rotterdam:" + queue + ":working",
                    "rotterdam:" + queue + ":jobs:pool"
                ]
                for queue in queues
            )),
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
                "rotterdam:" + queue + ":working",
                "rotterdam:" + queue + ":done",
                "rotterdam:" + queue + ":jobs:pool"
            ],
            args=args,
            client=self
        )

    client.qfinish = types.MethodType(qfinish, client)


def extend_redis(client):
    add_qadd(client)
    add_qpop(client)
    add_qfinish(client)
