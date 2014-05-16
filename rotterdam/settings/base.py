import textwrap


classes = []


class SettingMeta(type):

    def __new__(cls, name, bases, attrs):
        base_constructor = super(SettingMeta, cls).__new__

        parents = [
            base for base in bases
            if isinstance(base, SettingMeta)
        ]
        if not parents:
            return base_constructor(cls, name, bases, attrs)

        attrs["order"] = len(classes)

        new_class = base_constructor(cls, name, bases, attrs)

        docstring = textwrap.dedent(new_class.__doc__).strip()
        setattr(new_class, "desc", docstring)
        setattr(new_class, "short", docstring.splitlines()[0])

        classes.append(new_class)

        return new_class


class Setting(object):

    name = None
    section = None
    cli = None
    type = None
    action = None
    default = None
    short = None
    choices = None

    def __init__(self):
        self.value = None

        if self.default is not None:
            self.set(self.default)

    def add_option(self, parser):
        if not self.cli:
            return
        args = tuple(self.cli)

        help_txt = "%s [%s]" % (self.short, self.default)
        help_txt = help_txt.replace("%", "%%")

        kwargs = {
            "action": self.action or "store",
            "type": self.type or str,
            "default": None,
            "help": help_txt
        }

        if self.choices:
            kwargs['choices'] = self.choices

        if args[0].startswith("-"):
            kwargs['dest'] = self.name

        if kwargs["action"] != "store":
            kwargs.pop("type")

        parser.add_argument(*args, **kwargs)

    def get(self):
        return self.value

    def set(self, val):
        self.value = val

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return repr(self.value)

    def __lt__(self, other):
        return (self.section == other.section and
                self.order < other.order)

    __cmp__ = __lt__


Setting = SettingMeta('Setting', (Setting,), {})
