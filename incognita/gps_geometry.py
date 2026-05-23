"""Shared GPS geometry helpers: haversine distance and segment metrics on point series."""

import numpy as np
import pandas as pd

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


def add_speed_to_gdf(gdf: pd.DataFrame) -> pd.DataFrame:
    """Add time_diff, meters, speed_calc, and index columns (segment distance and speed).

    Args:
        gdf: Must have columns lon, lat, timestamp.
    """
    gdf["time_diff"] = pd.to_datetime(gdf["timestamp"])
    diff = gdf["time_diff"].diff(1)
    gdf["time_diff"] = diff.astype(int) / 10**9
    haversine_args = gdf["lat"].shift(1), gdf["lon"].shift(1), gdf.loc[1:, "lat"], gdf.loc[1:, "lon"]
    gdf["meters"] = get_haversine_dist(*haversine_args)
    gdf["speed_calc"] = gdf["meters"] / gdf["time_diff"]
    gdf["index"] = gdf.index
    return gdf
