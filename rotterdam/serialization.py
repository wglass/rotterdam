import datetime
import json

import dateutil.parser
import pytz


TIMESTAMP_PREFIX = "@T-"
INTERVAL_PREFIX = "@I-"


class DateAwareJSONEncoder(json.JSONEncoder):

    def default(self, value):
        if isinstance(value, datetime.datetime):
            return TIMESTAMP_PREFIX + value.isoformat()
        elif isinstance(value, datetime.timedelta):
            return INTERVAL_PREFIX + str(value.total_seconds())

        return super(DateAwareJSONEncoder, self).default(value)


class DateAwareJSONDecoder(json.JSONDecoder):

    def decode(self, value):
        return self.convert(json.loads(value))

    def convert(self, val):
        if isinstance(val, basestring) and val.startswith(TIMESTAMP_PREFIX):
            return dateutil.parser.parse(
                val[len(TIMESTAMP_PREFIX):], tzinfos={"UTC": pytz.utc}
            )
        elif isinstance(val, basestring) and val.startswith(INTERVAL_PREFIX):
            return datetime.timedelta(
                seconds=float(val[len(INTERVAL_PREFIX):])
            )
        elif isinstance(val, dict):
            val = {
                key: self.convert(value)
                for key, value in val.iteritems()
            }
        elif isinstance(val, list):
            val = map(self.convert, val)

        return val
