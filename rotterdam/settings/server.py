from .base import Setting

from ..features import is_available


def csv(cls, value):
    return value.split(",")


class Queues(Setting):
    """
    Comma-delimited list of queues.
    """

    name = "queues"
    cli = ["queues"]
    type = csv


class ConfigFile(Setting):
    """
    Location of the config file to use.
    """

    name = "config_file"
    cli = ["-f", "--config_file"]


class PIDFile(Setting):
    """
    Location of the PID file.
    """

    name = "pid_file"
    cli = ["-p", "--pid-file"]
    default = "/tmp/rotterdam.pid"


class ListenPort(Setting):
    """
    The port to listen for incoming jobs on.
    """

    name = "listen_port"
    cli = ["-l", "--listen-port"]
    type = int
    default = 8765


class RedisHost(Setting):
    """
    Redis host to keep the queue state in.

    Optionally allows for specifiying a port by
    including it after a ':' like so:

    localhost:2345
    """

    name = "redis_host"
    cli = ["-r", "--redis-host"]
    default = "localhost"


class NumConsumers(Setting):
    """
    Number of consumer processes to spawn.
    """

    name = "num_consumers"
    cli = ["-n", "--num-consumers"]
    type = int
    default = 6


class ShutdownGracePeriod(Setting):
    """
    Time in seconds to wait between a TERM and QUIT signal.
    """

    name = "shutdown_grace_period"
    cli = ["--shutdown-grace-period"]
    type = int
    default = 10


class HeartbeatInterval(Setting):
    """
    Heartbeat interval.
    """

    name = "heartbeat_interval"
    cli = ["--heartbeat-interval"]
    type = float
    default = 1.0


class Debug(Setting):
    """
    Turn on debugging
    """

    name = "debug"
    section = "Debugging"
    cli = ["-d", "--debug"]
    action = "store_true"
    default = False


if is_available("concurrency"):

    class Concurrency(Setting):
        """
        Number of green threads per process.
        """

        name = "concurrency"
        section = "Concurrency"
        cli = ["-c", "--concurrency"]
        type = int
