"""GPS-related geometric and time-series calculations (no I/O, no rendering)."""

import numpy as np
import pandas as pd

from incognita.observability import timed

METERS_PER_DEGREE = 111_000.0
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
        gdf: Must have columns [lon, lat, timestamp].
    Returns:
        Same DataFrame with added columns time_diff, meters, speed_calc, index.
    """
    gdf["time_diff"] = pd.to_datetime(gdf["timestamp"])
    diff = gdf["time_diff"].diff(1)
    gdf["time_diff"] = diff.astype(int) / 10**9  # convert to seconds
    haversine_args = gdf["lat"].shift(1), gdf["lon"].shift(1), gdf.loc[1:, "lat"], gdf.loc[1:, "lon"]
    gdf["meters"] = get_haversine_dist(*haversine_args)
    gdf["speed_calc"] = gdf["meters"] / gdf["time_diff"]
    gdf["index"] = gdf.index
    return gdf


def douglas_peucker(coords: np.ndarray, tolerance_meters: float) -> tuple[np.ndarray, np.ndarray]:
    """Douglas–Peucker simplification; returns (simplified_coords, keep_mask).

    keep_mask indexes into the original coords so callers can align timestamps.
    """
    if coords.shape[0] <= 2:
        keep = np.ones(coords.shape[0], dtype=bool)
        return coords.copy(), keep

    tol_deg = tolerance_meters / METERS_PER_DEGREE
    keep = np.zeros(coords.shape[0], dtype=bool)
    keep[0] = True
    keep[-1] = True
    stack: list[tuple[int, int]] = [(0, coords.shape[0] - 1)]

    while stack:
        start_idx, end_idx = stack.pop()
        if end_idx - start_idx <= 1:
            continue
        start = coords[start_idx]
        end = coords[end_idx]
        segment = end - start
        seg_len = np.hypot(segment[0], segment[1])
        if seg_len == 0.0:
            continue
        idx_range = np.arange(start_idx + 1, end_idx)
        pts = coords[idx_range]
        v = pts - start
        cross = np.abs(segment[0] * v[:, 1] - segment[1] * v[:, 0])
        dist = cross / seg_len
        max_idx_rel = int(np.argmax(dist))
        if float(dist[max_idx_rel]) > tol_deg:
            idx_keep = idx_range[max_idx_rel]
            keep[idx_keep] = True
            stack.append((start_idx, idx_keep))
            stack.append((idx_keep, end_idx))

    return coords[keep], keep


@timed
def get_stationary_groups(
    gdf: pd.DataFrame, max_time_diff: int = 30, max_dist_meters: int = 10
) -> pd.DataFrame:
    """Return groups of stationary points with basic aggregations."""
    gdf = gdf.copy()
    is_stationary = (
        (gdf["time_diff"] > max_time_diff) & (gdf["meters"] < max_dist_meters) & (gdf["meters"] != 0)
    )
    gdf["is_stationary"] = np.where(is_stationary, 1, 0)

    stationary_groups = (
        gdf.groupby(["is_stationary", gdf["is_stationary"].ne(gdf["is_stationary"].shift()).cumsum()])
        .agg(list)
        .reset_index(level=1, drop=True)
    )

    stationary_groups = stationary_groups[stationary_groups.index == 1]
    stationary_groups = stationary_groups[stationary_groups.lon.apply(len) > 1]
    stationary_groups["start"] = stationary_groups["timestamp"].apply(min)
    stationary_groups["end"] = stationary_groups["timestamp"].apply(max)
    stationary_groups["num_points"] = stationary_groups["lat"].apply(len)
    stationary_groups["lat"] = stationary_groups["lat"].apply(np.mean)
    stationary_groups["lon"] = stationary_groups["lon"].apply(np.mean)
    if "altitude" in stationary_groups.columns:
        stationary_groups["altitude"] = stationary_groups["altitude"].apply(lambda x: np.mean(np.array(x)))
    stationary_groups = stationary_groups.drop("timestamp", axis=1)

    return stationary_groups
