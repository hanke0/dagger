import multiprocessing
import os
import signal
import time
import sys

import setproctitle
import psutil

from dagger.logger import logger

HANDLED_SIGNALS = (
    signal.SIGINT,
    signal.SIGTERM,
)

__all__ = ("Supervisor",)


class Supervisor:
    def __init__(self, target, args=(), kwargs=None, name=None, worker_memory_limit=0):
        self.target = target
        self.args = args
        self.kwargs = kwargs if kwargs is not None else {}
        self.name = name
        self.should_exit = False
        self.worker_memory_limit = worker_memory_limit

    def handle_exit(self, sig, frame):
        self.should_exit = True

    def _run(self, *args, **kwargs):
        self._set_process_name(False)
        try:
            self.target(*args, **kwargs)
        except Exception as exc:
            logger.exception("worker [%d] got uncaught error %r", os.getpid(), exc)
            sys.exit(1)

    def start(self, workers=1):
        logger.info("Started master process [%d]", os.getpid())
        self._set_process_name(True)
        for sig in HANDLED_SIGNALS:
            signal.signal(sig, self.handle_exit)

        processes = []
        args = self.args
        kwargs = self.kwargs
        try:
            for idx in range(workers):
                process = multiprocessing.Process(target=self._run, args=args, kwargs=kwargs)
                process.start()
                processes.append(process)

            max_memory_allowed = self.worker_memory_limit
            while not self.should_exit:
                if not processes:
                    break

                for i in range(len(processes)):
                    process = processes[i]
                    if not process.is_alive():
                        process.join()
                        del processes[i]
                        break

                    if not max_memory_allowed:
                        continue

                    pid = process.pid
                    process = psutil.Process(pid)
                    mem = process.memory_info()
                    if mem.rss > max_memory_allowed:  # bytes
                        os.kill(pid, signal.SIGINT)
                        logger.warning("%s worker killed because memory overflowed", process)
                        process = multiprocessing.Process(target=self._run, args=args, kwargs=kwargs)
                        process.start()
                        processes.append(process)

                time.sleep(0.1)
        finally:
            logger.info("Stopping master process [%d]", os.getpid())
            for process in processes:
                process.terminate()

            for process in processes:
                process.join()

            logger.info("Exit master process [%d]", os.getpid())

    def _set_process_name(self, master: bool = False) -> bool:
        name = self.name
        if not name:
            return False
        if callable(name):
            name = name()
        suffix = "[master]" if master else "[worker]"
        name += suffix

        setproctitle.setproctitle(name)
        return True
