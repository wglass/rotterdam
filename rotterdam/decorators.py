import functools
import inspect


def job(queue, **kwargs):

    metadata = {
        "queue": queue,
        "unique": kwargs.get("unique", False),
        "delay": kwargs.get("delay", None)
    }

    def inner(fn):
        metadata['arg_names'] = inspect.getargspec(fn).args
        fn.job_metadata = metadata

        @functools.wraps(fn)
        def wrapper(*wrapped_args, **wrapped_kwargs):
            return fn(*wrapped_args, **wrapped_kwargs)

        return wrapper

    return inner
