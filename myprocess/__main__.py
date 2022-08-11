import logging
import multiprocessing
import os
import time
from typing import Callable, NoReturn

N_WORKERS = int(os.getenv("N_WORKERS", "4"))

logger = logging.getLogger(__name__)


def run_worker():
    from . import worker

    worker.work()


class MultiProcess:
    __procs: set[multiprocessing.Process]

    def __init__(self, target: Callable[[], None], n_workers: int) -> None:
        self.target = target
        self.n_workers = n_workers
        self.__procs = None
        self.all_dead_procs = []

    @property
    def procs(self) -> list[multiprocessing.Process]:
        return list(self.__procs or [])

    def _get_new_proc(self) -> multiprocessing.Process:
        return multiprocessing.Process(target=self.target)

    def __enter__(self) -> "MultiProcess":
        self.__procs = {self._get_new_proc() for _ in range(self.n_workers)}
        self.all_dead_procs = []
        for proc in self.__procs:
            logger.info("Starting process %s", proc)
            proc.start()
        return self

    def __exit__(self, *_, **__) -> bool:
        for proc in self.live_procs():
            logger.info("Terminating process %s", proc)
            proc.terminate()
        return False

    def dead_procs(self) -> list[multiprocessing.Process]:
        return [proc for proc in self.procs if not proc.is_alive()]

    def live_procs(self) -> list[multiprocessing.Process]:
        return [proc for proc in self.procs if proc.is_alive()]

    def maintain(self) -> NoReturn:
        if self.__procs is None:
            raise ValueError("The MultiProcess should be open before maintain")

        while True:
            self.live_procs()
            dead_procs = self.dead_procs()
            for dead_proc in dead_procs:
                self.all_dead_procs.append(dead_proc)
                logger.warning("Restarting dead process %s", dead_proc)
                self.__procs.remove(dead_proc)
                new_proc = self._get_new_proc()
                self.__procs.add(new_proc)
                new_proc.start()
            time.sleep(1)


def main():
    logging.basicConfig(
        level="INFO",
        format="[%(asctime)s][%(levelname)s][%(module)s:%(lineno)s][%(processName)s]\n%(message)s",
    )
    logger.info("Starting main process with %s", N_WORKERS)
    with MultiProcess(run_worker, N_WORKERS) as multiproc:
        try:
            multiproc.maintain()
        except KeyboardInterrupt:
            time.sleep(5)
            logger.info("Closing main process")
        if multiproc.all_dead_procs:
            logger.warning(
                "Some process died unexpectedly %s", multiproc.all_dead_procs
            )


if __name__ == "__main__":
    main()
