import logging
import os
import socket
import time
from collections.abc import Callable
from functools import wraps

import pandas as pd
import psutil
from rich.console import Console
from rich.table import Table

from incognita.data_models import GeoBoundingBox, GeoCoords

logger = logging.getLogger(__name__)
_console = Console()


BYTES_PER_MB = 1024 * 1024

# Hardcoded default map center (Berlin). Used for GPS page and as fallback when airport/city is unknown.
BERLIN_LAT = 52.52
BERLIN_LON = 13.405
DEFAULT_MAP_BOX = GeoBoundingBox(center=GeoCoords(BERLIN_LAT, BERLIN_LON), width=0.065, name="Berlin")


def get_ip_address() -> str:
    """Get the IP address of the current server."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 100))
    socket_name = s.getsockname()
    s.close()
    return socket_name[0]


def timed(func: Callable[..., object]) -> Callable[..., object]:
    """Prints execution time and memory usage for the decorated function."""

    @wraps(func)
    def wrapper(*args: object, **kwargs: object) -> object:
        process = psutil.Process(os.getpid())
        mem_before_mb = process.memory_info().rss / BYTES_PER_MB
        start = time.time()
        result = func(*args, **kwargs)
        elapsed_s = round(time.time() - start, 2)
        mem_after_mb = process.memory_info().rss / BYTES_PER_MB
        delta_mb = mem_after_mb - mem_before_mb
        table = Table(show_header=False, box=None)
        table.add_column("", style="cyan", width=28)
        table.add_column("", style="green", justify="right", width=8)
        table.add_column("", style="yellow")
        table.add_row(
            func.__name__,
            f"{elapsed_s} s",
            f"{mem_before_mb:.2f}MB → {mem_after_mb:.2f}MB  [magenta](Δ{delta_mb:.2f}MB)[/]",
        )
        _console.print(table)
        return result

    return wrapper


GOOGLE_SHEETS_DOCUMENT_ID = "1V4hVhSH1_tHizwqlSQ2ymysQwwQMuFENfE9lB5vJPQY"


def google_sheets_url(tab_name: str = "raw") -> str:
    return f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_DOCUMENT_ID}/gviz/tq?tqx=out:csv&sheet={tab_name}"


def google_sheets_view_url(tab_name: str = "raw") -> str:
    """URL to open the sheet in the browser (no export params)."""
    return f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_DOCUMENT_ID}/edit"


def df_from_gsheets(gsheets_url: str = google_sheets_url()) -> pd.DataFrame:
    return pd.read_csv(gsheets_url, keep_default_na=False)
