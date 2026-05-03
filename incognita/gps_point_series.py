"""Operations on time-ordered GPS point series: segment distance/speed, trip splitting, stationary groups."""

import logging

import numpy as np
import pandas as pd

from incognita.geo_distance import get_haversine_dist
from incognita.observability import timed

logger = logging.getLogger(__name__)


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


@timed
def get_stationary_groups(
    gdf: pd.DataFrame, max_time_diff: int = 30, max_dist_meters: int = 10
) -> pd.DataFrame:
    """Return groups of stationary points; each row has aggregations. Stationary = within max_dist_meters and max_time_diff."""
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
