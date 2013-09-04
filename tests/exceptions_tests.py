from rotterdam import exceptions


def test_each_exception_is_the_proper_subclass():

    def assert_is_proper_subclass(subclass):
        assert issubclass(subclass, exceptions.RotterdamError)

    for declared_item in dir(exceptions):
        if declared_item.startswith("__"):
            continue

        assert_is_proper_subclass.description = (
            "test_%s_is_proper_subclass" % declared_item
        )

        yield assert_is_proper_subclass, getattr(exceptions, declared_item)
