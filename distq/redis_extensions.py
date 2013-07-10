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
                queue + ":schedule",
                queue + ":working",
                queue + ":job_pool"
            ],
            args=[when, job_key, job_payload],
            client=self
        )

    client.qadd = types.MethodType(qadd, client)


def add_qget(client):
    content = get_script_content("qget")

    method = client.register_script(content)

    def qget(self, queue, cutoff, start=0):
        return method(
            keys=[
                queue + ":schedule",
                queue + ":job_pool"
            ],
            args=[start, cutoff],
            client=self
        )

    client.qget = types.MethodType(qget, client)


def add_qsetstate(client):
    content = get_script_content("qsetstate")

    method = client.register_script(content)

    def qsetstate(self, queue, current, target, *job_keys):
        args = [time.time()]
        args.extend(job_keys)

        return method(
            keys=[
                queue + ":" + current,
                queue + ":" + target
            ],
            args=args,
            client=self
        )

    client.qsetstate = types.MethodType(qsetstate, client)


def extend_redis(client):
    add_qadd(client)
    add_qget(client)
    add_qsetstate(client)


__all__ = [extend_redis]
