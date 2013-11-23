import functools
import inspect
import collections


def job(*args, **kwargs):

    metadata = {
        "queue": kwargs.get("queue", None),
        "unique": kwargs.get("unique", False),
        "delay": kwargs.get("delay", None)
    }

    if len(args) == 1 and isinstance(args[0], collections.Callable):
        fn = args[0]

        metadata['arg_names'] = inspect.getargspec(fn).args
        fn.job_metadata = metadata

        @functools.wraps(fn)
        def wrapper(*wrapped_args, **wrapped_kwargs):
            return fn(*wrapped_args, **wrapped_kwargs)

        return wrapper

    def inner(fn):
        metadata['arg_names'] = inspect.getargspec(fn).args
        fn.job_metadata = metadata

        @functools.wraps(fn)
        def wrapper(*wrapped_args, **wrapped_kwargs):
            return fn(*wrapped_args, **wrapped_kwargs)

        return wrapper

    return inner
