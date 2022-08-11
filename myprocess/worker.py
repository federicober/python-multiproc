import logging
import random
import signal
import sys
import time
from typing import Callable

SimpleCallable = Callable[[], None]

logger = logging.getLogger(__name__)

TASKS: list[tuple[SimpleCallable, float]] = []


def get_task() -> SimpleCallable:
    funcs, weights = zip(*TASKS)
    return random.choices(funcs, weights=weights, k=1)[0]


def task(weight: float = 1) -> Callable[[SimpleCallable], SimpleCallable]:
    assert weight > 0

    def deco(func: SimpleCallable) -> SimpleCallable:
        TASKS.append((func, weight))
        return func

    return deco


def random_int() -> int:
    return random.randint(1, 10)


@task(10)
def long_wait():
    sleep_time = random.randint(1, 10)
    logger.info("Sleeping for %s secs", sleep_time)
    time.sleep(sleep_time)


@task(3)
def raise_known_error() -> None:
    logger.info("Run known failing task")
    raise ValueError("Fake unexpected error")


@task(1)
def raise_unexpected_error() -> None:
    logger.info("Run system error")
    sys.exit(1)


def convert_to_sigint(signum: int, __):
    logger.info("Received signal %s", signum)
    raise KeyboardInterrupt()


def work() -> None:
    signal.signal(signal.SIGTERM, convert_to_sigint)

    try:
        while True:
            task = get_task()
            logger.debug("Running task %s", task.__name__)
            try:
                task()
            except ValueError:
                logger.exception("")
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Exiting worker")


if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    work()
