import errno
import logging
import os
import Queue
import select
import signal
import socket
import sys
import time
from multiprocessing import queues

import redis
import setproctitle

from .config import Config
from .worker import Worker
from .job_iterator import JobIterator


class Master(object):

    understood_signals = [
        "TTIN", "TTOU",
        "HUP", "USR1",
        "INT", "QUIT", "TERM",
        "CHLD"
    ]

    def __init__(self, config_file):
        self.pid = None
        self.pid_file_path = None
        self.reexec_pid = 0

        self.name = "master"

        self.logger = logging.getLogger(__name__)

        self.config_file = config_file
        self.load_config()

        self.workers = {}

        self.socket = None

        self.job_queue = queues.Queue()
        self.results_queue = queues.Queue()

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

    def setup_datastore(self):
        if ":" in self.config.master.redis:
            host, port = self.config.master.redis.split(":")
            self.redis = redis.Redis(host=host, port=port)
        else:
            self.redis = redis.Redis(host=self.config.master.redis)

    def setup_socket(self):
        try:
            os.unlink(self.config.master.listen)
        except OSError:
            if os.path.exists(self.config.master.listen):
                raise

        self.logger.info("Listening on %s", self.config.master.listen)

        if ":" in self.config.master.listen:
            host, port = self.config.master.listen.split(":")
            self.socket = socket.socket(socket.AF_INET)
            self.socket.setblocking(0)
            self.socket.bind((host, int(port)))
        else:
            self.socket = socket.socket(socket.AF_UNIX)
            self.socket.setblocking(0)
            self.socket.bind(self.config.master.listen)

        self.socket.listen(5)

    def setup_signals(self):
        for signal_name in self.understood_signals:
            signal.signal(
                getattr(signal, "SIG%s" % signal_name),
                getattr(self, "handle_%s" % signal_name.lower())
            )

    def run(self):
        self.pid = os.getpid()
        self.logger.info("Starting %s (%d)", self.name, int(self.pid))

        self.setup_pid_file()

        self.setup_datastore()

        self.deserialize_queue()

        self.setup_socket()
        self.setup_signals()

        setproctitle.setproctitle("distq: %s" % self.name)

        for i in range(self.number_of_workers):
            self.spawn_worker()

        while True:
            try:
                (input_sources, [], []) = select.select(
                    [self.socket, self.results_queue._reader],
                    [], [],
                    self.config.master.heartbeat_interval
                )

                if not input_sources:
                    self.heartbeat()
                    continue

                if input_sources[0] == self.socket:
                    self.handle_payload()

                if (
                    input_sources[0] == self.results_queue._reader
                    or len(input_sources) > 1
                ):
                    self.handle_worker_result()

            except select.error as e:
                if e.args[0] not in [errno.EAGAIN, errno.EINTR]:
                    raise
            except OSError as e:
                if e.errno not in [errno.EAGAIN, errno.EINTR]:
                    raise
            except IOError as e:
                if e.errno not in [errno.EBADF]:
                    raise
            except SystemExit:
                raise
            except KeyboardInterrupt:
                self.handle_quit()
            except Exception, e:
                self.logger.exception(e)
                sys.exit(-1)

    def handle_ttin(self, *args):
        self.number_of_workers += 1
        self.logger.info(
            "Upping number of workers to %d",
            self.number_of_workers
        )
        self.spawn_worker()

    def handle_ttou(self, *args):
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

    def handle_hup(self, *args):
        self.logger.info("Got HUP, reloading config")
        old_address = self.config.master.listen

        workers_by_age = sorted(
            self.workers.items(),
            key=lambda i: i[1].age
        )

        self.load_config()

        if self.config.master.listen != old_address:
            self.socket.close()
            self.setup_socket()

        for i in range(self.number_of_workers):
            self.spawn_worker()

        while len(workers_by_age) > 0:
            (worker_pid, _) = workers_by_age.pop(0)
            self.send_signal(signal.SIGQUIT, worker_pid)

    def handle_usr1(self, *args):
        os.rename(
            self.pid_file_path,
            self.pid_file_path + ".old." + str(self.pid)
        )
        self.reexec_pid = os.fork()

        if self.reexec_pid != 0:
            # old master proc
            self.name = "old master"
            self.pid_file_path += ".old." + str(self.pid)
            setproctitle.setproctitle("distq: %s" % self.name)
            self.wind_down()
            return

        os.chdir(self.launch_context["cwd"])

        os.execvpe(
            self.launch_context["executable"],
            self.launch_context["args"],
            os.environ
        )

    def handle_chld(self, *args):
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

    def handle_int(self, *args):
        self.broadcast_signal(signal.SIGINT)

    def handle_quit(self, *args):
        self.socket.close()
        self.wind_down()

    def handle_term(self, *args):
        self.socket.close()
        self.wind_down(signal_to_broadcast=signal.SIGTERM)

    def spawn_worker(self):
        worker = Worker(
            self.job_queue,
            self.results_queue,
            self.config.workers
        )

        pid = os.fork()

        if pid != 0:
            self.workers[pid] = worker
            return

        # in the worker process now
        try:
            setproctitle.setproctitle("distq: worker")
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

    def serialize_queue(self):
        self.logger.debug("serializing remaining queue")
        jobs = []
        while not self.job_queue.empty():
            jobs.append(self.job_queue.get())

        self.logger.debug(jobs)

    def deserialize_queue(self):
        self.logger.debug("deserializing remaining queue")
        pass

    def wind_down(self, signal_to_broadcast=signal.SIGQUIT):
        self.logger.info("Winding down %s", self.name)
        self.serialize_queue()
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

    def handle_payload(self):
        conn, addr = self.socket.accept()

        for job in JobIterator(conn):
            self.logger.debug(
                "got job: %s",
                job
            )
            self.job_queue.put_nowait(job)

        conn.close()

    def handle_worker_result(self):
        while True:
            try:
                result = self.results_queue.get_nowait()

                self.logger.debug(
                    "Job completed in %0.2fs seconds",
                    result["time"]
                )

            except Queue.Empty:
                break
