import errno
import os
import signal
import sys

from .injector import Injector
from .consumer import Consumer


class WorkerCollection(object):

    def __init__(self, boss):
        self.boss = boss
        self.injectors = {}
        self.consumers = {}

    def pop(self, pid):
        if pid in self.injectors:
            return self.injectors.pop(pid)
        elif pid in self.consumers:
            return self.consumers.pop(pid)

        raise KeyError

    def __len__(self):
        return len(self.injectors) + len(self.consumers)

    def iteritems(self):
        for injector_pid, injector in self.injectors.iteritems():
            yield injector_pid, injector
        for consumer_pid, consumer in self.consumers.iteritems():
            yield consumer_pid, consumer

    @property
    def consumer_count(self):
        return len(self.consumers)

    @consumer_count.setter
    def consumer_count(self, new_size):
        while len(self.consumers) > new_size:
            self.remove_worker(Consumer)
        while len(self.consumers) < new_size:
            self.add_worker(Consumer)

    @property
    def injector_count(self):
        return len(self.injectors)

    @injector_count.setter
    def injector_count(self, new_size):
        while len(self.injectors) > new_size:
            self.remove_worker(Injector)
        while len(self.injectors) < new_size:
            self.add_worker(Injector)

    def add_worker(self, worker_class):
        worker = worker_class(self.boss)

        pid = os.fork()

        if pid != 0:
            if worker_class == Consumer:
                self.consumers[pid] = worker
            elif worker_class == Injector:
                self.injectors[pid] = worker
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
        for worker_pid in self.consumers:
            self.send_signal(worker_pid, signal.SIGINT)

    def broadcast(self, signal):
        for injector_pid in self.injectors:
            self.send_signal(signal, injector_pid)
        for consumer_pid in self.consumers:
            self.send_signal(signal, consumer_pid)

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
        for worker_pid in (self.injectors.keys() + self.consumers.keys()):
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
