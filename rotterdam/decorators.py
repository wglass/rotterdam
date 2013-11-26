import functools


def job(queue, unique=False, delay=None):

    def inner(fn):
        fn.job_metadata = {
            "queue": queue,
            "unique": unique,
            "delay": delay
        }

        @functools.wraps(fn)
        def wrapper(*wrapped_args, **wrapped_kwargs):
            return fn(*wrapped_args, **wrapped_kwargs)

        return wrapper

    return inner
