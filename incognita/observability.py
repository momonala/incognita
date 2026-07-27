"""Shared logging and lightweight runtime instrumentation helpers."""

import logging
import os
import time
from collections.abc import Callable
from functools import wraps

import psutil
from spyglass import MetricsCollector
from spyglass import configure_logging as _spyglass_configure_logging

from incognita.config import SPYGLASS_HOST

BYTES_PER_MB = 1024 * 1024

metrics = MetricsCollector(host=SPYGLASS_HOST, project="incognita")


def configure_logging(level: int = logging.INFO) -> None:
    """Configure application logging from entrypoints only.

    Attaches a SpyglassHandler (via spyglass.configure_logging) so log
    records are also forwarded to the Spyglass server, not just stdout.
    """
    _spyglass_configure_logging(host=SPYGLASS_HOST, project="incognita", level=level)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)


def timed(func: Callable[..., object]) -> Callable[..., object]:
    """Log execution time and memory usage at debug level, and report both to spyglass."""

    @wraps(func)
    def wrapper(*args: object, **kwargs: object) -> object:
        logger = logging.getLogger(func.__module__)
        process = psutil.Process(os.getpid())
        mem_before_mb = process.memory_info().rss / BYTES_PER_MB
        start = time.time()
        result = func(*args, **kwargs)
        elapsed_s = time.time() - start
        mem_after_mb = process.memory_info().rss / BYTES_PER_MB
        mem_delta_mb = mem_after_mb - mem_before_mb
        logger.debug(
            "[timing] %s elapsed_s=%.2f mem_before_mb=%.2f mem_after_mb=%.2f delta_mb=%.2f",
            func.__name__,
            elapsed_s,
            mem_before_mb,
            mem_after_mb,
            mem_delta_mb,
        )
        stat_prefix = f"{metrics.project}.{func.__name__}"
        metrics.timing(f"{stat_prefix}.duration_ms", elapsed_s * 1000, prefix=False)
        metrics.gauge(f"{stat_prefix}.mem_delta_mb", mem_delta_mb, prefix=False)
        return result

    return wrapper
