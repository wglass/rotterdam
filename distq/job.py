import json


class Job(object):

    def __init__(self, payload):
        payload = json.loads(payload)

        self.spec = payload['spec']
        self.args = payload['args']
