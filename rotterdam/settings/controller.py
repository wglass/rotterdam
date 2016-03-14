from .base import Setting
from .common import ConfigFile, Debug  # noqa


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
