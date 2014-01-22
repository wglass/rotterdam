import functools


def job(queue_name, unique=False, delay=None):

    def inner(fn):
        fn.job_metadata = {
            "queue_name": queue_name,
            "unique": unique,
            "delay": delay
        }

        @functools.wraps(fn)
        def wrapper(*wrapped_args, **wrapped_kwargs):
            return fn(*wrapped_args, **wrapped_kwargs)

        return wrapper

    return inner
