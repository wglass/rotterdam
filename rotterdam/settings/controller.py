from .base import Setting


class ConfigFile(Setting):
    """
    Location of the config file to use.
    """

    name = "config_file"
    cli = ["-c", "--config_file"]


class Command(Setting):
    """
    The command to send to the rotterdam process.
    """

    name = "command"
    cli = ["command"]
    choices = [
        "stop", "reload", "relaunch", "expand", "contract", "pause"
    ]


class PIDFile(Setting):
    """
    Location of the PID file.
    """

    name = "pid_file"
    cli = ["-p", "--pid-file"]
    default = "/tmp/rotterdam.pid"


class Debug(Setting):
    """
    Turn on debugging
    """

    name = "debug"
    section = "Debugging"
    cli = ["-d", "--debug"]
    action = "store_true"
    default = False
