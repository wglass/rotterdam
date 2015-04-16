from .base import Setting


class ConfigFile(Setting):
    """
    Location of the config file to use.
    """

    name = "config_file"
    cli = ["-f", "--config_file"]


class Debug(Setting):
    """
    Turn on debugging
    """

    name = "debug"
    section = "Debugging"
    cli = ["-d", "--debug"]
    action = "store_true"
    default = False
