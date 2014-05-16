from unittest import TestCase
from mock import Mock, patch, call
from nose.tools import eq_, assert_raises

from rotterdam import job, NoSuchJob, InvalidPayload

import datetime
import json
import time

from rotterdam.payload import Payload


@job("testqueue")
def test_job_func(*args):
    pass


@job("testqueue", unique=True)
def test_unique_job_func(*args, **kwargs):
    pass


@job("testqueue", delay=datetime.timedelta(hours=2))
def test_delayed_two_hours_job(*args):
    pass


class JobTests(TestCase):

    def test_default_attributes_are_all_none(self):
        job = Payload()

        assert job.module is None
        assert job.func is None
        assert job.args is None
        assert job.kwargs is None
        assert job.call is None

        assert job.when is None
        assert job.unique_key is None

    def test_deserialize_sets_instance_attributes(self):
        now = int(time.time())

        message = json.dumps({
            "module": test_job_func.__module__,
            "func": test_job_func.__name__,
            "args": ["foo", "bar"],
            "kwargs": {"derp": "hork"},
            "unique_key": "adfe999",
            "when": now
        })

        job = Payload.deserialize(message)

        eq_(job.module, test_job_func.__module__)
        eq_(job.func, test_job_func.__name__)
        eq_(job.args, ["foo", "bar"])
        eq_(job.kwargs, {"derp": "hork"})
        eq_(job.call, test_job_func)
        eq_(job.unique_key, "adfe999")
        eq_(job.when, now)

    def test_deserializing_junk_raises_invalid_payload(self):
        self.assertRaises(
            InvalidPayload,
            Payload.deserialize, "asdfweawe;awe;"
        )

    @patch("rotterdam.payload.time")
    def test_deserialize_delayed_job(self, mock_time):
        now = time.time()

        mock_time.time.return_value = now

        message = json.dumps({
            "module": test_delayed_two_hours_job.__module__,
            "func": test_delayed_two_hours_job.__name__
        })

        job = Payload.deserialize(message)

        eq_(job.when, now + datetime.timedelta(hours=2).total_seconds())

    def test_loading_a_bogus_function_raises_exception(self):
        message = json.dumps({"module": "foo", "func": "bar"})

        assert_raises(
            NoSuchJob,
            Payload.deserialize, message
        )

    def test_run_calls_the_call_attribute(self):
        message = json.dumps({
            "module": test_job_func.__module__,
            "func": test_job_func.__name__
        })

        job = Payload.deserialize(message)

        job.call = Mock()
        job.args = ["foo", "bar"]
        job.kwargs = {"derp": "hork"}

        job.run()

        job.call.assert_called_once_with("foo", "bar", derp="hork")

    def test_repr_string(self):
        message = json.dumps({
            "module": test_job_func.__module__,
            "func": test_job_func.__name__
        })

        job = Payload.deserialize(message)

        job.module = "some.module"
        job.func = "a_func"

        job.args = ["foo", "bar"]
        job.kwargs = {"derp": "hork"}

        eq_(
            repr(job),
            "some.module.a_func(foo, bar, derp=hork)"
        )

    def test_serialize_default_job_to_json_string(self):
        job = Payload()

        eq_(
            json.loads(job.serialize()),
            {
                "module": None,
                "func": None,
                "args": None,
                "kwargs": None,
                "unique_key": None,
                "when": None
            }
        )

    def test_serialize_to_json_string(self):
        now = int(time.time())
        job = Payload()

        job.module = "some.module"
        job.func = "a_func"

        job.args = ["foo", "bar"]
        job.kwargs = {"derp": "hork"}

        job.unique_key = "adfe999"

        job.when = now

        eq_(
            json.loads(job.serialize()),
            {
                "module": "some.module",
                "func": "a_func",
                "args": ["foo", "bar"],
                "kwargs": {"derp": "hork"},
                "unique_key": "adfe999",
                "when": now
            }
        )

    def test_deserialize_ignores_extra_junk(self):
        now = int(time.time())

        job = Payload.deserialize(
            json.dumps({
                "module": test_job_func.__module__,
                "func": test_job_func.__name__,
                "args": ["foo", "bar"],
                "kwargs": {"derp": "hork"},
                "unique_key": "adfe999",
                "when": now,
                "hack": "where 1=1; drop table customers; --"
            })
        )

        eq_(job.call, test_job_func)
        eq_(job.args, ["foo", "bar"])
        eq_(job.kwargs, {"derp": "hork"})
        eq_(job.unique_key, "adfe999")
        eq_(job.when, now)

    def test_serialize_and_deserialize_are_stable(self):
        now = int(time.time())

        message = json.dumps({
            "module": test_job_func.__module__,
            "func": test_job_func.__name__,
            "args": ["foo", "bar"],
            "kwargs": {"derp": "hork"},
            "unique_key": "adfe999",
            "when": now,
            "hack": "where 1=1; drop table customers; --"
        })

        job = Payload.deserialize(message)
        job.deserialize(job.serialize())
        job.deserialize(job.serialize())

        eq_(
            json.loads(job.serialize()),
            {
                "module": test_job_func.__module__,
                "func": test_job_func.__name__,
                "args": ["foo", "bar"],
                "kwargs": {"derp": "hork"},
                "unique_key": "adfe999",
                "when": now
            }
        )

    @patch("rotterdam.payload.os")
    @patch("rotterdam.payload.time")
    @patch("rotterdam.payload.random")
    @patch("rotterdam.payload.hashlib")
    def test_non_unique_job_unique_key_includes_context(
            self, hashlib, random, mock_time, os
    ):
        now = time.time()

        random.random.return_value = 1010
        os.getpid.return_value = 777
        mock_time.time.return_value = now

        message = json.dumps({
            "module": test_job_func.__module__,
            "func": test_job_func.__name__,
            "args": ["foo", "bar"],
            "kwargs": {"bar": 1234},
        })

        job = Payload.deserialize(message)

        hashlib.md5().assert_has_calls([
            call.update(test_job_func.__module__),
            call.update(test_job_func.__name__),
            call.update("testqueue"),
            call.update('foo'),
            call.update('bar'),
            call.update("bar=1234"),
            call.update(str(now)),
            call.update('777'),
            call.update('1010'),
            call.hexdigest()
        ])

        eq_(job.unique_key, hashlib.md5().hexdigest.return_value)

    @patch("rotterdam.payload.hashlib")
    def test_unique_job_excludes_context_in_unique_key(self, hashlib):
        message = json.dumps({
            "module": test_unique_job_func.__module__,
            "func": test_unique_job_func.__name__,
            "args": ["foo", "bar"],
            "kwargs": {"bar": 1234},
        })

        job = Payload.deserialize(message)

        hashlib.md5().assert_has_calls([
            call.update(test_unique_job_func.__module__),
            call.update(test_unique_job_func.__name__),
            call.update("testqueue"),
            call.update('foo'),
            call.update('bar'),
            call.update("bar=1234"),
            call.hexdigest()
        ])

        eq_(job.unique_key, hashlib.md5().hexdigest.return_value)
