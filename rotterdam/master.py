import errno
import os
import signal
import sys
import time
from multiprocessing import queues

import redis
import setproctitle

from .connection import Connection
from .proc import Proc
from .arbiter import Arbiter
from .injector import Injector
from .consumer import Consumer
from .worker_set import WorkerSet
from .redis_extensions import extend_redis


class Master(Proc):

    signal_map = {
        "hup": "reload_config",
        "usr1": "relaunch",
        "ttin": "expand_consumers",
        "ttou": "contract_consumers",
        "tstp": "halt_current_jobs",
        "term": "wind_down_gracefully",
        "quit": "wind_down_immediately",
        "chld": "handle_worker_exit"
    }

    def __init__(self, config):
        super(Master, self).__init__()

        self.pid_file_path = None
        self.reexec_pid = 0

        self.config = config

        self.injectors = WorkerSet(self, Injector)
        self.arbiters = WorkerSet(self, Arbiter)
        self.consumers = WorkerSet(self, Consumer)

        self.conn = None
        self.redis = None

        self.ready_queue = None
        self.results_queue = None

        self.wind_down_time = None

        args = sys.argv[:]
        args.insert(0, sys.executable)

        self.launch_context = {
            "executable": sys.executable,
            "args": args,
            "cwd": os.getcwd()
        }

    def load_config(self):
        self.config.load()
        self.pid_file_path = self.config.pid_file

    def setup_pid_file(self):
        if not os.path.isdir(os.path.dirname(self.pid_file_path)):
            raise RuntimeError(
                "%s does not exist! Can't create pid file" % self.pid_file_path
            )

        if os.path.exists(self.pid_file_path):
            raise RuntimeError(
                (
                    "pid file (%s) already exists! \n"
                    + "If no %s process is running it could be stale."
                ) % (self.name, self.pid_file_path)
            )

        fd = open(self.pid_file_path, 'w')
        try:
            fd.write(u"%s\n" % self.pid)
        except Exception:
            os.unlink(self.pid_file_path)
            raise
        else:
            fd.close()

    def setup_connection(self):
        self.conn = Connection(port=self.config.listen_port)
        self.conn.open()
        self.logger.info("Listening on port %s", self.config.listen_port)

    def setup_redis(self):
        if ":" in self.config.redis_host:
            host, port = self.config.redis_host.split(":")
            self.redis = redis.Redis(host=host, port=port)
        else:
            self.redis = redis.Redis(host=self.config.redis_host)

        extend_redis(self.redis)

    def setup_ipc_queues(self):
        self.ready_queue = queues.Queue()
        self.results_queue = queues.Queue()

    def setup(self):
        super(Master, self).setup()
        self.load_config()
        self.setup_pid_file()
        self.setup_ipc_queues()
        self.setup_connection()
        self.setup_redis()

    def run(self):
        super(Master, self).run()
        self.injectors.count = 2
        self.arbiters.count = 1
        self.consumers.count = self.config.num_consumers

        while True:
            try:
                signal.pause()
            except KeyboardInterrupt:
                self.wind_down_gracefully()
            except SystemExit:
                raise
            except Exception:
                self.logger.exception("Error during main loop!")
                self.wind_down_immediately()
                sys.exit(-1)

    def expand_consumers(self, expansion_signal, *args):
        new_count = self.consumers.count + 1
        self.logger.info("Expanding number of consumers to %d", new_count)
        self.consumers.count = new_count
        self.arbiters.broadcast(expansion_signal)

    def contract_consumers(self, contraction_signal, *args):
        if self.consumers.count <= 1:
            self.logger.info(
                "Ignoring contraction, number of consumers already at %d",
                self.consumers.count
            )
            return

        new_count = self.consumers.count - 1
        self.logger.info("Contracting number of consumers to %d", new_count)
        self.consumers.count = new_count
        self.arbiters.broadcast(contraction_signal)

    def reload_config(self, *args):
        self.logger.info("Reloading config")
        old_port = self.config.listen_port

        self.load_config()

        if self.config.listen_port != old_port:
            self.conn.close()
            self.setup_connection()

        self.injectors.count = 2
        self.arbiters.count = 1
        self.consumers.count = self.config.num_consumers

    def relaunch(self, *args):
        os.rename(
            self.pid_file_path,
            self.pid_file_path + ".old." + str(self.pid)
        )

        self.reexec_pid = os.fork()

        if self.reexec_pid != 0:
            self.pid_file_path += ".old." + str(self.pid)
            setproctitle.setproctitle("rotterdam: old %s" % self.name)
            self.wind_down_gracefully()
            return

        os.environ["ROTTERDAM_SOCKET_FD"] = str(self.conn.socket.fileno())

        os.chdir(self.launch_context["cwd"])

        os.execvpe(
            self.launch_context["executable"],
            self.launch_context["args"],
            os.environ
        )

    def broadcast(self, sig):
        self.injectors.broadcast(sig)
        self.arbiters.broadcast(sig)
        self.consumers.broadcast(sig)

    def regroup(self, regenerate=True):
        self.injectors.regroup(regenerate=regenerate)
        self.arbiters.regroup(regenerate=regenerate)
        self.consumers.regroup(regenerate=regenerate)

    def handle_worker_exit(self, *args):
        if not self.wind_down_time:
            self.regroup()
            return

        self.regroup(regenerate=False)
        if (
                self.injectors.count == 0
                and self.arbiters.count == 0
                and self.consumers.count == 0
        ):
            try:
                os.unlink(self.pid_file_path)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    pass
            sys.exit(0)
        else:
            time.sleep(self.wind_down_time - time.time())
            self.broadcast(signal.SIGKILL)

    def halt_current_jobs(self, *args):
        self.logger.info("Toggling job processing")
        self.consumers.broadcast(signal.SIGTSTP)

    def wind_down_gracefully(self, *args):
        self.wind_down(graceful=True)

    def wind_down_immediately(self, *args):
        self.wind_down(graceful=False)

    def wind_down(self, graceful=True):
        if graceful:
            self.logger.info("Winding down gracefully.")
        else:
            self.logger.info("Winding down IMMEDIATELY.")
        self.conn.close()
        self.wind_down_time = (time.time() + self.config.shutdown_grace_period)
        self.broadcast(signal.SIGTERM if graceful else signal.SIGQUIT)
