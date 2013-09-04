from unittest import TestCase
from mock import Mock, patch, call
from nose.tools import eq_, assert_raises

from rotterdam import NoSuchJob

import json
import time

from rotterdam.job import Job


def test_job_func(*args):
    pass


class JobTests(TestCase):

    def test_default_attributes(self):
        job = Job()

        assert job.module is None
        assert job.function is None
        assert job.args is None
        assert job.kwargs is None

        assert job.when is None
        assert job.unique_key is None

    def test_loading_a_bogus_function_raises_exception(self):
        job = Job()

        job.module = "somefakemodule"

        assert_raises(
            NoSuchJob,
            job.load
        )

    def test_load_sets_the_call_attribute(self):
        job = Job()

        job.module = test_job_func.__module__
        job.function = test_job_func.__name__

        assert job.call is None

        job.load()

        eq_(job.call, test_job_func)

    def test_run_calls_the_call_attribute(self):
        job = Job()

        job.call = Mock()
        job.args = ["foo", "bar"]
        job.kwargs = {"derp": "hork"}

        job.run()

        job.call.assert_called_once_with("foo", "bar", derp="hork")

    def test_repr_string(self):
        job = Job()

        job.module = "some.module"
        job.function = "a_func"

        job.args = ["foo", "bar"]
        job.kwargs = {"derp": "hork"}

        eq_(
            repr(job),
            "some.module.a_func(foo, bar, derp=hork)"
        )

    def test_serialize_default_job_to_json_string(self):
        job = Job()

        eq_(
            json.loads(job.serialize()),
            {
                "module": None,
                "function": None,
                "args": None,
                "kwargs": None,
                "unique_key": None,
                "when": None
            }
        )

    def test_serialize_to_json_string(self):
        now = int(time.time())
        job = Job()

        job.module = "some.module"
        job.function = "a_func"

        job.args = ["foo", "bar"]
        job.kwargs = {"derp": "hork"}

        job.unique_key = "adfe999"

        job.when = now

        eq_(
            json.loads(job.serialize()),
            {
                "module": "some.module",
                "function": "a_func",
                "args": ["foo", "bar"],
                "kwargs": {"derp": "hork"},
                "unique_key": "adfe999",
                "when": now
            }
        )

    def test_deserialize(self):
        now = int(time.time())
        job = Job()

        job.deserialize(
            json.dumps({
                "module": "some.module",
                "function": "a_func",
                "args": ["foo", "bar"],
                "kwargs": {"derp": "hork"},
                "unique_key": "adfe999",
                "when": now
            })
        )

        eq_(job.module, "some.module")
        eq_(job.function, "a_func")
        eq_(job.args, ["foo", "bar"])
        eq_(job.kwargs, {"derp": "hork"})
        eq_(job.unique_key, "adfe999")
        eq_(job.when, now)

    def test_deserialize_ignores_extra_junk(self):
        now = int(time.time())
        job = Job()

        job.deserialize(
            json.dumps({
                "module": "some.module",
                "function": "a_func",
                "args": ["foo", "bar"],
                "kwargs": {"derp": "hork"},
                "unique_key": "adfe999",
                "when": now,
                "hack": "where 1=1; drop table customers; --"
            })
        )

        eq_(job.module, "some.module")
        eq_(job.function, "a_func")
        eq_(job.args, ["foo", "bar"])
        eq_(job.kwargs, {"derp": "hork"})
        eq_(job.unique_key, "adfe999")
        eq_(job.when, now)

    def test_serialize_and_deserialize_are_stable(self):
        now = int(time.time())
        job = Job()

        payload = json.dumps({
            "module": "some.module",
            "function": "a_func",
            "args": ["foo", "bar"],
            "kwargs": {"derp": "hork"},
            "unique_key": "adfe999",
            "when": now,
            "hack": "where 1=1; drop table customers; --"
        })

        job.deserialize(payload)
        job.deserialize(job.serialize())
        job.deserialize(job.serialize())
        payload = json.loads(job.serialize())

        eq_(
            payload,
            {
                "module": "some.module",
                "function": "a_func",
                "args": ["foo", "bar"],
                "kwargs": {"derp": "hork"},
                "unique_key": "adfe999",
                "when": now
            }
        )

    def test_from_function(self):
        job = Job()

        job.from_function(test_job_func, ["foo"], {"bar": 1234})

        eq_(job.module, "tests.job_tests")
        eq_(job.function, "test_job_func")
        eq_(job.args, ["foo"])
        eq_(job.kwargs, {"bar": 1234})

    def test_from_function_via_a_string(self):
        job = Job()

        job.from_function("some.module:a_func", ["foo"], {"bar": 1234})

        eq_(job.module, "some.module")
        eq_(job.function, "a_func")
        eq_(job.args, ["foo"])
        eq_(job.kwargs, {"bar": 1234})

    def test_from_function_via_a_unicode_string(self):
        job = Job()

        job.from_function(u"some.module:a_func", ["foo"], {"bar": 1234})

        eq_(job.module, "some.module")
        eq_(job.function, "a_func")
        eq_(job.args, ["foo"])
        eq_(job.kwargs, {"bar": 1234})

    @patch("rotterdam.job.hashlib")
    def test_from_function_uniqueness_comes_from_md5_of_spec(self, hashlib):
        job = Job()

        job.from_function(u"some.module:a_func", ["foo", "bar"], {"bar": 1234})

        hashlib.md5().assert_has_calls([
            call.update('some.module'),
            call.update('a_func'),
            call.update('foo'),
            call.update('bar'),
            call.update("bar=1234"),
            call.hexdigest()
        ])

        eq_(job.unique_key, hashlib.md5().hexdigest.return_value)
