import logging
import socket
import time
from functools import wraps

import pandas as pd
from geopy import geocoders
from joblib import Memory

from incognita.data_models import GeoBoundingBox, GeoCoords

logger = logging.getLogger(__name__)

disk_memory = Memory("cache")


def get_ip_address() -> str:
    """Get the IP address of the current server."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 100))
    socket_name = s.getsockname()
    s.close()
    return socket_name[0]


@disk_memory.cache
def coordinates_from_place_name(city: str) -> GeoBoundingBox:
    coordinate_fetcher = geocoders.GeoNames("momonala")
    city_location = coordinate_fetcher.geocode(city)
    return GeoBoundingBox(
        center=GeoCoords(city_location.latitude, city_location.longitude),
        width=0.065,
        name=city_location.address,
    )


def timed(func):
    """This decorator prints the execution time for the decorated function."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logger.info(
            "TIMED: {}s for {}".format(
                round(end - start, 2),
                func.__name__,
            )
        )
        return result

    return wrapper


def google_sheets_url(tab_name: str = "raw") -> str:
    document_id = "1V4hVhSH1_tHizwqlSQ2ymysQwwQMuFENfE9lB5vJPQY"
    return f"https://docs.google.com/spreadsheets/d/{document_id}/gviz/tq?tqx=out:csv&sheet={tab_name}"


def df_from_gsheets(gsheets_url: str = google_sheets_url()) -> pd.DataFrame:
    return pd.read_csv(gsheets_url, keep_default_na=False)
