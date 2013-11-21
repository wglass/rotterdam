import errno
import os
import signal
import sys

from .unloader import Unloader
from .loader import Loader


class Union(object):

    def __init__(self, boss):
        self.boss = boss
        self.loaders = {}
        self.unloaders = {}

    def pop(self, pid):
        if pid in self.loaders:
            return self.loaders.pop(pid)
        elif pid in self.unloaders:
            return self.unloaders.pop(pid)

        raise KeyError

    def __len__(self):
        return len(self.loaders) + len(self.unloaders)

    def iteritems(self):
        for loader_pid, loader in self.loaders.iteritems():
            yield loader_pid, loader
        for unloader_pid, unloader in self.unloaders.iteritems():
            yield unloader_pid, unloader

    @property
    def unloader_count(self):
        return len(self.unloaders)

    @unloader_count.setter
    def unloader_count(self, new_size):
        while len(self.unloaders) > new_size:
            self.remove_worker(Unloader)
        while len(self.unloaders) < new_size:
            self.add_worker(Unloader)

    @property
    def loader_count(self):
        return len(self.loaders)

    @loader_count.setter
    def loader_count(self, new_size):
        while len(self.loaders) > new_size:
            self.remove_worker(Loader)
        while len(self.loaders) < new_size:
            self.add_worker(Loader)

    def add_worker(self, worker_class):
        worker = worker_class(self.boss)

        pid = os.fork()

        if pid != 0:
            if worker_class == Unloader:
                self.unloaders[pid] = worker
            elif worker_class == Loader:
                self.loaders[pid] = worker
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

    def pause_work(self):
        for worker_pid in self.unloaders:
            self.send_signal(worker_pid, signal.SIGINT)

    def broadcast(self, signal):
        for loader_pid in self.loaders:
            self.send_signal(signal, loader_pid)
        for unloader_pid in self.unloaders:
            self.send_signal(signal, unloader_pid)

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
        for worker_pid in (self.loaders.keys() + self.unloaders.keys()):
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
                self.pop(worker_pid)
            except KeyError:
                continue
