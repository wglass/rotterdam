import errno
import os
import signal
import sys


class Team(object):

    def __init__(self, master, worker_class):
        self.master = master
        self.worker_class = worker_class
        self.workers = {}

    @property
    def size(self):
        return len(self.workers)

    def set_size(self, new_size):
        while len(self.workers) > new_size:
            self.remove_worker()
        while len(self.workers) < new_size:
            self.add_worker()

    def add_worker(self):
        worker = self.worker_class(self.master)

        pid = os.fork()

        if pid != 0:
            self.workers[pid] = worker
            return

        try:
            worker.run()
            sys.exit(0)
        except SystemExit:
            raise
        except Exception:
            self.master.logger.exception(
                "Unhandled exception in %s process", worker.name
            )
            sys.exit(-1)
        finally:
            self.master.logger.info("%s process exiting", worker.name)

    def remove_worker(self):
        (oldest_worker_pid, _) = sorted(
            self.workers.items(),
            key=lambda i: i[1].age
        ).pop(0)

        self.workers.pop(oldest_worker_pid)
        self.send_signal(signal.SIGQUIT, oldest_worker_pid)

    def broadcast(self, signal):
        for worker_pid in self.workers:
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

    def regroup(self, regenerate=True):
        exited_worker_pids = []
        for worker_pid in self.workers:
            try:
                pid, _ = os.waitpid(worker_pid, os.WNOHANG)
                if pid == worker_pid:
                    exited_worker_pids.append(worker_pid)
            except OSError as e:
                if e.errno == errno.ECHILD:
                    self.workers.pop(worker_pid)
                raise

        for worker_pid in exited_worker_pids:
            try:
                self.workers.pop(worker_pid)
            except KeyError:
                pass
            if regenerate:
                self.add_worker()
