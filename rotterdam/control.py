import errno
import signal
import os

from .config import Config


class Control(object):

    def __init__(self, args):
        self.args = args
        self.pid = None

    def load_config(self):
        self.config = Config(self.args.config_file)
        self.config.load()

    def get_pid(self):
        try:
            pid_file = open(self.config.master.pid_file, 'r')
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise RuntimeError("master process not currently running!")
            else:
                raise

        self.pid = int(pid_file.read())

        pid_file.close()

    def run(self):
        self.load_config()
        self.get_pid()

        getattr(self, self.args.command)()

    def stop(self):
        os.kill(self.pid, signal.SIGTERM)

    def reload(self):
        os.kill(self.pid, signal.SIGHUP)

    def relaunch(self):
        os.kill(self.pid, signal.SIGUSR1)

    def expand(self):
        os.kill(self.pid, signal.SIGTTIN)

    def contract(self):
        os.kill(self.pid, signal.SIGTTOU)

    def pause(self):
        os.kill(self.pid, signal.SIGTSTP)
