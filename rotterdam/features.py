available = set()


try:
    import gevent
    available.add("concurrency")
except ImportError:
    pass


def is_available(feature):
    return bool(feature in available)
