import errno
import os
import signal
import sys

from .unloader import Unloader
from .loader import Loader
from .arbiter import Arbiter


class Union(object):

    def __init__(self, boss):
        self.boss = boss
        self.workers_by_class = {
            Unloader: {},
            Loader: {},
            Arbiter: {}
        }

    def pop(self, pid):
        for worker_xref in self.workers_by_class.values():
            if pid in worker_xref:
                return worker_xref.pop(pid)

        raise KeyError

    def __len__(self):
        return sum([
            len(worker_xref)
            for worker_xref in self.workers_by_class.values()
        ])

    def iteritems(self):
        for worker_xref in self.workers_by_class.values():
            for pid, worker in worker_xref.iteritems():
                yield pid, worker

    @property
    def unloader_count(self):
        return len(self.workers_by_class[Unloader])

    @unloader_count.setter
    def unloader_count(self, new_size):
        self.sync_count(Unloader, new_size)

    @property
    def loader_count(self):
        return len(self.workers_by_class[Loader])

    @loader_count.setter
    def loader_count(self, new_size):
        self.sync_count(Loader, new_size)

    @property
    def arbiter_count(self):
        return len(self.workers_by_class[Arbiter])

    @arbiter_count.setter
    def arbiter_count(self, new_size):
        self.sync_count(Arbiter, new_size)

    def sync_count(self, worker_class, new_size):
        while len(self.workers_by_class[worker_class]) > new_size:
            self.remove_worker(worker_class)

        while len(self.workers_by_class[worker_class]) < new_size:
            self.add_worker(worker_class)

    def add_worker(self, worker_class):
        worker = worker_class.onboard(self.boss)

        pid = os.fork()

        if pid != 0:
            self.workers_by_class[worker_class][pid] = worker
            return

        try:
            worker.setup()
            worker.run()
            sys.exit(0)
        except SystemExit:
            raise
        except:
            self.boss.logger.exception(
                "Unhandled exception in %s process!" % worker.name
            )
            sys.exit(-1)
        finally:
            self.boss.logger.info("%s exiting" % worker.name)

    def remove_worker(self, worker_class):
        (oldest_worker_pid, worker) = sorted(
            self.workers_by_class[worker_class].items(),
            key=lambda i: i[1].age
        ).pop(0)

        self.workers_by_class[worker_class].pop(oldest_worker_pid)
        self.send_signal(signal.SIGQUIT, oldest_worker_pid)

    def broadcast(self, signal):
        for worker_class in self.workers_by_class.keys():
            self.broadcast_to(worker_class, signal)

    def broadcast_to(self, worker_class, signal):
        for worker_pid in self.workers_by_class[worker_class].keys():
            self.send_signal(signal, worker_pid)

    def send_signal(self, signal, worker_pid):
        try:
            os.kill(worker_pid, signal)
        except OSError as error:
            if error.errno == errno.ESRCH:
                try:
                    self.pop(worker_pid)
                except KeyError:
                    return
            raise

    def regroup(self):
        pids_to_pop = []
        for worker_xref in self.workers_by_class.values():
            for worker_pid in worker_xref:
                try:
                    pid, status = os.waitpid(worker_pid, os.WNOHANG)
                    if pid == worker_pid:
                        pids_to_pop.append(worker_pid)
                except OSError as e:
                    if e.errno == errno.ECHILD:
                        worker_xref.pop(worker_pid)
                    raise

        for worker_pid in pids_to_pop:
            try:
                self.pop(worker_pid)
            except KeyError:
                continue
