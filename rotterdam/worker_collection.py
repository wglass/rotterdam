import errno
import os
import signal
import sys


class WorkerCollection(object):

    def __init__(self, master, worker_class):
        self.master = master
        self.worker_class = worker_class
        self.workers = {}

    @property
    def count(self):
        return len(self.workers)

    @count.setter
    def count(self, new_size):
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
        except:
            self.master.logger.exception(
                "Unhandled exception in %s process (%d)" % (worker.name, pid)
            )
            sys.exit(-1)
        finally:
            self.master.logger.info("%s (%d) exiting", worker.name, pid)

    def remove_worker(self):
        (oldest_worker_pid, worker) = sorted(
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

    def regroup(self):
        pids_to_pop = []
        for worker_pid in self.workers:
            try:
                pid, status = os.waitpid(worker_pid, os.WNOHANG)
                if pid == worker_pid:
                    pids_to_pop.append(worker_pid)
            except OSError as e:
                if e.errno == errno.ECHILD:
                    self.pop(worker_pid)
                raise

        for worker_pid in pids_to_pop:
            try:
                self.workers.pop(worker_pid)
            except KeyError:
                continue