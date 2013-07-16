import time

def some_job(arg1, arg2, foo=None):
    time.sleep(2)
    print "arg1: %s" % arg1
    print "arg2: %s" % arg2
    print "foo: %s" % foo
