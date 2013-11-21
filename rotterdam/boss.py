import errno
import logging
import os
import signal
import sys
import time
from multiprocessing import queues

import redis
import setproctitle

from .config import Config
from .connection import Connection
from .loader import Loader
from .unloader import Unloader
from .arbiter import Arbiter
from .redis_extensions import extend_redis


class Boss(object):

    signal_map = {
        "hup": "reload_config",
        "usr1": "relaunch",
        "ttin": "expand_unloaders",
        "ttou": "contract_unloaders",
        "int": "halt_current_jobs",
        "quit": "wind_down_gracefully",
        "term": "wind_down_immediately",
        "chld": "handle_worker_exit"
    }

    def __init__(self, config_file):
        self.pid = None
        self.pid_file_path = None
        self.reexec_pid = 0

        self.name = "boss"

        self.logger = logging.getLogger(__name__)

        self.config_file = config_file

        self.loaders = {}
        self.arbiters = {}
        self.unloaders = {}

        self.connection = None
        self.redis = None

        self.ready_queue = None
        self.taken_queue = None
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
        self.config = Config(self.config_file)
        self.config.load()
        self.pid_file_path = self.config.master.pid_file
        self.number_of_unloaders = self.config.master.num_unloaders

    def setup_pid_file(self):
        if not os.path.isdir(os.path.dirname(self.pid_file_path)):
            raise RuntimeError(
                "%s does not exist! Can't create pid file" % self.pid_file_path
            )

        if os.path.exists(self.pid_file_path):
            raise RuntimeError(
                (
                    "pid file (%s) already exists! \n"
                    "If no boss process is running it could be stale."
                ) % self.pid_file_path
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
        self.connection = Connection(
            port=self.config.master.listen_port
        )
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

    def setup_signals(self):
        for signal_name, handler_name in self.signal_map.iteritems():
            signal.signal(
                getattr(signal, "SIG%s" % signal_name.upper()),
                getattr(self, handler_name)
            )

    def setup_ipc_queues(self):
        self.ready_queue = queues.Queue(maxsize=None)
        self.taken_queue = queues.Queue()
        self.results_queue = queues.Queue()

    def run(self):
        self.pid = os.getpid()
        self.logger.info("Starting %s (%d)", self.name, int(self.pid))

        self.load_config()
        self.setup_pid_file()
        self.setup_ipc_queues()
        self.setup_connection()
        self.setup_redis()
        self.setup_signals()

        setproctitle.setproctitle("rotterdam: %s" % self.name)

        self.spawn_worker(Loader, sources={"connection": self.connection})
        self.spawn_worker(
            Arbiter,
            sources={
                "taken": self.taken_queue,
                "results": self.results_queue
            },
            outputs={
                "ready": self.ready_queue
            }
        )
        for i in range(self.number_of_unloaders):
            self.spawn_worker(
                Unloader,
                sources={
                    'ready': self.ready_queue
                },
                outputs={
                    'taken': self.taken_queue,
                    'results': self.results_queue
                }
            )

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

    def expand_unloaders(self, expansion_signal, *args):
        self.number_of_unloaders += 1
        self.logger.info(
            "Upping number of unloaders to %d",
            self.number_of_unloaders
        )
        self.spawn_worker(
            Unloader,
                sources={
                    'ready': self.ready_queue
                },
                outputs={
                    'taken': self.taken_queue,
                    'results': self.results_queue
                }
        )
        self.broadcast_signal(expansion_signal, worker_class=Arbiter)

    def contract_unloaders(self, contraction_signal, *args):
        if self.number_of_unloaders <= 1:
            self.logger.info(
                "Ignoring TTOU, number of unloaders already at %d",
                self.number_of_unloaders
            )
            return

        self.number_of_unloaders -= 1
        self.logger.info(
            "Contracting number of unloaders to %d",
            self.number_of_unloaders
        )

        self.broadcast_signal(contraction_signal, worker_class=Arbiter)

        # get the pid of the oldest unloader by
        # sorting by age and popping the first one
        oldest_unloader_pid, _ = sorted(
            self.unloaders.items(),
            key=lambda i: i[1].age
        ).pop(0)

        self.send_signal(signal.SIGQUIT, oldest_unloader_pid)

    def reload_config(self, *args):
        self.logger.info("Reloading config")
        old_port = self.config.master.listen_port

        unloaders_by_age = sorted(
            self.unloaders.items(),
            key=lambda i: i[1].age
        )

        self.load_config()

        if self.config.master.listen_port != old_port:
            self.connection.close()
            self.setup_connection()

        for i in range(self.number_of_unloaders):
            self.spawn_worker(
                Unloader,
                sources={
                    'ready': self.ready_queue
                },
                outputs={
                    'taken': self.taken_queue,
                    'results': self.results_queue
                }
            )

        while len(unloaders_by_age) > 0:
            (unloader_pid, _) = unloaders_by_age.pop(0)
            self.send_signal(signal.SIGQUIT, unloader_pid)

    def relaunch(self, *args):
        os.rename(
            self.pid_file_path,
            self.pid_file_path + ".old." + str(self.pid)
        )

        self.reexec_pid = os.fork()

        if self.reexec_pid != 0:
            # old boss proc
            self.name = "old boss"
            self.pid_file_path += ".old." + str(self.pid)
            setproctitle.setproctitle("rotterdam: %s" % self.name)
            self.wind_down()
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
        for worker_xref in [self.loaders, self.unloaders, self.arbiters]:
            for worker_pid in worker_xref.keys():
                try:
                    pid, status = os.waitpid(worker_pid, os.WNOHANG)
                    if pid == worker_pid:
                        worker_xref.pop(worker_pid)
                except OSError as e:
                    if e.errno == errno.ECHILD:
                        worker_xref.pop(worker_pid)
                    raise

        if self.wind_down_time:
            if not any([self.loaders, self.unloaders, self.arbiters]):
                try:
                    os.unlink(self.pid_file_path)
                except OSError as e:
                    if e.errno == errno.ENOENT:
                        pass
                sys.exit(0)
            else:
                time.sleep(self.wind_down_time - time.time())
                self.broadcast_signal(signal.SIGKILL)

    def halt_current_jobs(self, *args):
        self.broadcast_signal(signal.SIGINT, worker_class=Unloader)

    def wind_down_gracefully(self, *args):
        self.connection.close()
        self.wind_down()

    def wind_down_immediately(self, *args):
        self.connection.close()
        self.wind_down(signal_to_broadcast=signal.SIGTERM)

    def spawn_worker(self, worker_class, sources=None, outputs=None):
        name = worker_class.__name__.lower()

        worker = worker_class(self.config.master, self.redis, sources, outputs)

        pid = os.fork()

        if pid != 0:
            getattr(self, name + "s")[pid] = worker
            return

        try:
            setproctitle.setproctitle("rotterdam: %s" % name)
            self.logger.info("Starting up %s" % name)
            worker.setup()
            worker.run()
            sys.exit(0)
        except SystemExit:
            raise
        except:
            self.logger.exception("Unhandled exception in %s process!" % name)
            sys.exit(-1)
        finally:
            self.logger.info("%s exiting" % name)

    def wind_down(self, signal_to_broadcast=signal.SIGQUIT):
        self.logger.info("Winding down %s", self.name)
        self.wind_down_time = (
            time.time() + self.config.master.shutdown_grace_period
        )
        self.broadcast_signal(signal_to_broadcast)

    def broadcast_signal(self, signal, worker_class=None):
        if not worker_class:
            worker_pids = (
                self.loaders.keys()
                + self.unloaders.keys()
                + self.arbiters.keys()
            )
        else:
            name = worker_class.__name__.lower()
            worker_pids = getattr(self, name + "s").keys()

        for worker_pid in worker_pids:
            self.send_signal(signal, worker_pid)

    def send_signal(self, signal, worker_pid):
        try:
            os.kill(worker_pid, signal)
        except OSError as error:
            if error.errno == errno.ESRCH:
                for worker_xref in [
                        self.loaders, self.arbiters, self.unloaders
                ]:
                    try:
                        self.worker_xref.pop(worker_pid)
                    except KeyError:
                        return
            raise

    def heartbeat(self):
        pass
