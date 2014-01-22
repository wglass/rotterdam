from .decorators import job

import time


@job("audience")
def non_unique_job(arg1, foo=None):
    time.sleep(1)
    print "this was my arg: %s" % arg1
    print "this was my kwarg: %s" % foo

@job("audience", unique=True)
def unique_job(arg):
    time.sleep(3)
    print "unique job arg: %s" % arg


@job("response", unique=["arg1", "arg2"])
def semi_unique_job(arg1, arg2, foo=None):
    time.sleep(2)
    print "arg1: %s" % arg1
    print "arg2: %s" % arg2
    print "foo: %s" % foo
