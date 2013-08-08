import errno
import logging
import os
import signal
import sys
import time
from multiprocessing import queues

import setproctitle

from .config import Config
from .connection import Connection
from .worker import Worker
from .arbiter import Arbiter


class Master(object):

    signal_map = {
        "hup": "reload_config",
        "usr1": "relaunch",
        "ttin": "expand_workers",
        "ttou": "contract_workers",
        "int": "halt_current_jobs",
        "quit": "wind_down_gracefully",
        "term": "wind_down_immediately",
        "chld": "handle_worker_exit"
    }

    def __init__(self, config_file):
        self.pid = None
        self.pid_file_path = None
        self.reexec_pid = 0

        self.name = "master"

        self.logger = logging.getLogger(__name__)

        self.config_file = config_file

        self.workers = {}

        self.connection = None

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
        self.number_of_workers = self.config.master.num_workers

    def setup_pid_file(self):
        if not os.path.isdir(os.path.dirname(self.pid_file_path)):
            raise RuntimeError(
                "%s does not exist! Can't create pid file" % self.pid_file_path
            )

        if os.path.exists(self.pid_file_path):
            raise RuntimeError(
                (
                    "pid file (%s) already exists! \n"
                    "If no master is running it could be stale."
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

    def setup_signals(self):
        for signal_name, handler_name in self.signal_map.iteritems():
            signal.signal(
                getattr(signal, "SIG%s" % signal_name.upper()),
                getattr(self, handler_name)
            )

    def setup_ipc_queues(self):
        self.ready_queue = queues.Queue(
            maxsize=(
                self.number_of_workers * self.config.workers.greenlet_pool_size
            )
        )
        self.taken_queue = queues.Queue()
        self.results_queue = queues.Queue()

    def run(self):
        self.pid = os.getpid()
        self.logger.info("Starting %s (%d)", self.name, int(self.pid))

        self.load_config()
        self.setup_pid_file()
        self.setup_ipc_queues()
        self.setup_connection()
        self.setup_signals()

        setproctitle.setproctitle("rotterdam: %s" % self.name)

        for i in range(self.number_of_workers):
            self.spawn_worker()

        arbiter = Arbiter(
            self.config.arbiter,
            self.connection,
            self.ready_queue,
            self.taken_queue,
            self.results_queue
        )

        arbiter.setup()

        while True:
            try:
                arbiter.arbitrate(
                    timeout=self.config.master.heartbeat_interval
                )
                self.heartbeat()
            except SystemExit:
                raise
            except KeyboardInterrupt:
                self.wind_down_gracefully()
            except Exception, e:
                self.logger.exception(e)
                sys.exit(-1)

    def expand_workers(self, *args):
        self.number_of_workers += 1
        self.logger.info(
            "Upping number of workers to %d",
            self.number_of_workers
        )
        self.spawn_worker()

    def contract_workers(self, *args):
        if self.number_of_workers <= 1:
            self.logger.info(
                "Ignoring TTOU, number of workers already at %d",
                self.number_of_workers
            )
            return

        self.number_of_workers -= 1
        self.logger.info(
            "Contracting number of workers to %d",
            self.number_of_workers
        )

        # get the pid of the oldest worker by
        # sorting by age and popping the first one
        oldest_worker_pid, _ = sorted(
            self.workers.items(),
            key=lambda i: i[1].age
        ).pop(0)

        self.send_signal(signal.SIGQUIT, oldest_worker_pid)

    def reload_config(self, *args):
        self.logger.info("Reloading config")
        old_port = self.config.master.listen_port

        workers_by_age = sorted(
            self.workers.items(),
            key=lambda i: i[1].age
        )

        self.load_config()

        if self.config.master.listen_port != old_port:
            self.connection.close()
            self.setup_connection()

        for i in range(self.number_of_workers):
            self.spawn_worker()

        while len(workers_by_age) > 0:
            (worker_pid, _) = workers_by_age.pop(0)
            self.send_signal(signal.SIGQUIT, worker_pid)

    def relaunch(self, *args):
        os.rename(
            self.pid_file_path,
            self.pid_file_path + ".old." + str(self.pid)
        )

        self.reexec_pid = os.fork()

        if self.reexec_pid != 0:
            # old master proc
            self.name = "old master"
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
        worker_pids = self.workers.keys()
        for worker_pid in worker_pids:
            try:
                pid, status = os.waitpid(worker_pid, os.WNOHANG)
                if pid == worker_pid:
                    self.workers.pop(worker_pid)
            except OSError as e:
                if e.errno == errno.ECHILD:
                    self.workers.pop(worker_pid)
                raise

        if self.wind_down_time:
            if not self.workers:
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
        self.broadcast_signal(signal.SIGINT)

    def wind_down_gracefully(self, *args):
        self.connection.close()
        self.wind_down()

    def wind_down_immediately(self, *args):
        self.connection.close()
        self.wind_down(signal_to_broadcast=signal.SIGTERM)

    def spawn_worker(self):
        worker = Worker(
            self.ready_queue,
            self.taken_queue,
            self.results_queue,
            self.config.workers
        )

        pid = os.fork()

        if pid != 0:
            self.workers[pid] = worker
            return

        # in the worker process now
        try:
            setproctitle.setproctitle("rotterdam: worker")
            self.logger.info("Starting up worker")
            worker.setup()
            worker.run()
            sys.exit(0)
        except SystemExit:
            raise
        except:
            self.logger.exception("Exception in worker process!")
            sys.exit(-1)
        finally:
            self.logger.info("Worker exiting")

    def wind_down(self, signal_to_broadcast=signal.SIGQUIT):
        self.logger.info("Winding down %s", self.name)
        self.wind_down_time = (
            time.time() + self.config.master.shutdown_grace_period
        )
        self.broadcast_signal(signal_to_broadcast)

    def broadcast_signal(self, signal):
        for worker_pid in self.workers.keys():
            self.send_signal(signal, worker_pid)

    def send_signal(self, signal, worker_pid):
        try:
            os.kill(worker_pid, signal)
        except OSError as error:
            if error.errno == errno.ESRCH:
                try:
                    self.workers.pop(worker_pid)
                except KeyError:
                    return
            raise

    def heartbeat(self):
        pass
