import logging
import socket
import time
from functools import wraps

from geopy import geocoders

from incognita.data_models import GeoBoundingBox, GeoCoords

logger = logging.getLogger(__name__)


def get_ip_address() -> str:
    """Get the IP address of the current server."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 100))
    socket_name = s.getsockname()
    s.close()
    return socket_name[0]


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
        logger.debug("{} ran in {}s".format(func.__name__, round(end - start, 2)))
        return result

    return wrapper
