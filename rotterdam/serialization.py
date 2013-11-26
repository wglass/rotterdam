import datetime
import json

import dateutil.parser
import pytz


class DateAwareJSONEncoder(json.JSONEncoder):

    def default(self, value):
        if isinstance(value, datetime.datetime):
            return "@T-" + value.isoformat()
        elif isinstance(value, datetime.timedelta):
            return "@I-" + str(value.total_seconds())

        return super(DateAwareJSONEncoder, self).default(value)


class DateAwareJSONDecoder(json.JSONDecoder):

    def decode(self, value):
        return self.convert(json.loads(value))

    def convert(self, value):
        if isinstance(value, basestring) and value.startswith("@T-"):
            return dateutil.parser.parse(value[3:], tzinfos={"UTC": pytz.utc})
        elif isinstance(value, basestring) and value.startswith("@I-"):
            return datetime.timedelta(seconds=float(value[3:]))
        elif isinstance(value, dict):
            for key, val in value.iteritems():
                converted = self.convert(val)
                if converted != val:
                    value[key] = converted
        elif isinstance(value, list):
            for index, val in enumerate(value):
                converted = self.convert(val)
                if converted != val:
                    value[index] = converted

        return value


__all__ = [
    DateAwareJSONEncoder,
    DateAwareJSONDecoder
]
