import json


class Job(object):

    def __init__(self, payload):
        self.spec = None
        self.args = None

    def serialize(self):
        return json.dumps({
            "spec": self.spec,
            "args": self.args
        })

    def deserialize(self, payload):
        payload = json.loads(payload)

        self.spec = payload['spec']
        self.args = payload['args']

    def __repr__(self):
        "%s(%s)" % (self.spec, ", ".join(self.args))
