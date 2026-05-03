"""Shared logging and lightweight runtime instrumentation helpers."""

import logging
import os
import time
from collections.abc import Callable
from functools import wraps

import psutil

LOG_FORMAT = "%(asctime)s:%(levelname)s:%(name)s:%(message)s"
BYTES_PER_MB = 1024 * 1024


def configure_logging(level: int = logging.INFO) -> None:
    """Configure application logging from entrypoints only."""
    logging.basicConfig(level=level, format=LOG_FORMAT)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)


def timed(func: Callable[..., object]) -> Callable[..., object]:
    """Log execution time and memory usage at debug level."""

    @wraps(func)
    def wrapper(*args: object, **kwargs: object) -> object:
        logger = logging.getLogger(func.__module__)
        process = psutil.Process(os.getpid())
        mem_before_mb = process.memory_info().rss / BYTES_PER_MB
        start = time.time()
        result = func(*args, **kwargs)
        elapsed_s = time.time() - start
        mem_after_mb = process.memory_info().rss / BYTES_PER_MB
        logger.debug(
            "[timing] %s elapsed_s=%.2f mem_before_mb=%.2f mem_after_mb=%.2f delta_mb=%.2f",
            func.__name__,
            elapsed_s,
            mem_before_mb,
            mem_after_mb,
            mem_after_mb - mem_before_mb,
        )
        return result

    return wrapper
