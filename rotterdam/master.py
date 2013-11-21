import errno
import os
import signal
import sys
import time

import redis
import setproctitle

from .config import Config
from .connection import Connection
from .proc import Proc
from .worker_collection import WorkerCollection
from .redis_extensions import extend_redis


class Master(Proc):

    signal_map = {
        "hup": "reload_config",
        "usr1": "relaunch",
        "ttin": "expand_consumers",
        "ttou": "contract_consumers",
        "int": "halt_current_jobs",
        "quit": "wind_down_gracefully",
        "term": "wind_down_immediately",
        "chld": "handle_worker_exit"
    }

    def __init__(self, config_file):
        super(Master, self).__init__()

        self.pid_file_path = None
        self.reexec_pid = 0

        self.config_file = config_file

        self.workers = WorkerCollection(self)

        self.connection = None
        self.redis = None

        self.wind_down_time = None

        args = sys.argv[:]
        args.insert(0, sys.executable)

        self.launch_context = {
            "executable": sys.executable,
            "args": args,
            "cwd": os.getcwd()
        }

    def load_config(self):
        self.config = Config(self.config_file)
        self.config.load()
        self.pid_file_path = self.config.master.pid_file

    def setup_pid_file(self):
        if not os.path.isdir(os.path.dirname(self.pid_file_path)):
            raise RuntimeError(
                "%s does not exist! Can't create pid file" % self.pid_file_path
            )

        if os.path.exists(self.pid_file_path):
            raise RuntimeError(
                (
                    "pid file (%s) already exists! \n"
                    "If no %s process is running it could be stale."
                ) % (self.name, self.pid_file_path)
            )

        fd = open(self.pid_file_path, 'w')
        try:
            fd.write(u"%s\n" % self.pid)
        except:
            os.unlink(self.pid_file_path)
            raise
        else:
            fd.close()

    def setup_connection(self):
        self.connection = Connection(port=self.config.master.listen_port)
        self.connection.open()
        self.logger.info(
            "Listening on port %s", self.config.master.listen_port
        )

    def setup_redis(self):
        if ":" in self.config.master.redis_host:
            host, port = self.config.master.redis_host.split(":")
            self.redis = redis.Redis(host=host, port=port)
        else:
            self.redis = redis.Redis(host=self.config.master.redis_host)

        extend_redis(self.redis)

    def setup(self):
        super(Master, self).setup()
        self.load_config()
        self.setup_pid_file()
        self.setup_connection()
        self.setup_redis()

    def run(self):
        super(Master, self).run()

        self.workers.injector_count = self.config.master.num_injectors
        self.workers.consumer_count = self.config.master.num_consumers

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
        self.logger.info(
            "Upping number of consumers to %d",
            self.workers.consumer_count + 1
        )
        self.workers.consumer_count += 1

    def contract_consumers(self, contraction_signal, *args):
        if self.workers.consumer_count <= 1:
            self.logger.info(
                "Ignoring TTOU, number of consumers already at %d",
                self.workers.consumer_count
            )
            return

        self.logger.info(
            "Contracting number of consumers to %d",
            self.workers.consumer_count - 1
        )

        self.workers.consumer_count -= 1

    def reload_config(self, *args):
        self.logger.info("Reloading config")
        old_port = self.config.master.listen_port

        self.load_config()

        if self.config.master.listen_port != old_port:
            self.connection.close()
            self.setup_connection()

        self.workers.injector_count = self.config.master.num_injectors
        self.workers.consumer_count = self.config.master.num_consumers

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

        os.environ["ROTTERDAM_SOCKET_FD"] = str(
            self.connection.socket.fileno()
        )

        os.chdir(self.launch_context["cwd"])

        os.execvpe(
            self.launch_context["executable"],
            self.launch_context["args"],
            os.environ
        )

    def handle_worker_exit(self, *args):
        self.workers.regroup()

        if self.wind_down_time:
            if len(self.workers) == 0:
                try:
                    os.unlink(self.pid_file_path)
                except OSError as e:
                    if e.errno == errno.ENOENT:
                        pass
                sys.exit(0)
            else:
                time.sleep(self.wind_down_time - time.time())
                self.workers.broadcast(signal.SIGKILL)

    def halt_current_jobs(self, *args):
        self.workers.pause_work()

    def wind_down_gracefully(self, *args):
        self.logger.info("Winding down")
        self.connection.close()
        self.wind_down_time = (
            time.time() + self.config.master.shutdown_grace_period
        )
        self.workers.broadcast(signal.SIGQUIT)

    def wind_down_immediately(self, *args):
        self.logger.info("Winding down IMMEDIATELY")
        self.connection.close()
        self.wind_down_time = (
            time.time() + self.config.master.shutdown_grace_period
        )
        self.workers.broadcast(signal.SIGTERM)
