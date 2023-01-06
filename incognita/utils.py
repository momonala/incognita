import socket

from geopy import geocoders

from incognita.data_models import GeoBoundingBox, GeoCoords


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
