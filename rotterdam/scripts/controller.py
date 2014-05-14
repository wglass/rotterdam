import logging
import os
import signal

from rotterdam.config import Config


signal_map = {
    "stop": signal.SIGQUIT,
    "reload": signal.SIGHUP,
    "relaunch": signal.SIGUSR1,
    "expand": signal.SIGTTIN,
    "contract": signal.SIGTTOU
}


def get_pid(config):
    if not os.path.isfile(config.pid_file):
        raise RuntimeError("master process not currently running!")

    pid = None

    with open(config.pid_file, 'r') as pid_file:
        pid = int(pid_file.read())

    return pid


def run():
    config = Config.create('controller')
    args = config.load()

    if config.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    pid = get_pid(config)

    command_signal = signal_map[args.command]

    os.kill(pid, command_signal)
