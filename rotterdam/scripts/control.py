import argparse
import errno
import os
import signal
import sys

from rotterdam.config import Config


parser = argparse.ArgumentParser(description="Rotterdam control utility")

parser.add_argument(
    "config_file", type=str, help="Location of the config file to load"
)
parser.add_argument(
    "command", type=str,
    choices=["stop", "reload", "relaunch", "expand", "contract"],
    help="One of stop, reload, relaunch, expand or contract"
)


signal_map = {
    "stop": signal.SIGQUIT,
    "reload": signal.SIGHUP,
    "relaunch": signal.SIGUSR1,
    "expand": signal.SIGTTIN,
    "contract": signal.SIGTTOU
}


def get_pid(config):
    try:
        pid_file = open(config.master.pid_file, 'r')
    except IOError as e:
        if e.errno == errno.ENOENT:
            raise RuntimeError("master process not currently running!")
        else:
            raise

    pid = int(pid_file.read())

    pid_file.close()

    return pid


def run():
    args = parser.parse_args(sys.argv[1:])

    config = Config(args.config_file)
    config.load()

    pid = get_pid(config)

    command_signal = signal_map[args.command]

    os.kill(pid, command_signal)
