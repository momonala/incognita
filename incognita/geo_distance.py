"""Great-circle (haversine) distance between geographic coordinates."""

import numpy as np

RADIUS_EARTH_M = 6_378_137


def get_haversine_dist(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance between two points in meters (WGS84)."""
    delta_lat = lat2 * np.pi / 180 - lat1 * np.pi / 180
    delta_lon = lon2 * np.pi / 180 - lon1 * np.pi / 180
    a = np.sin(delta_lat / 2) * np.sin(delta_lat / 2) + np.cos(lat1 * np.pi / 180) * np.cos(
        lat2 * np.pi / 180
    ) * np.sin(delta_lon / 2) * np.sin(delta_lon / 2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return RADIUS_EARTH_M * c
