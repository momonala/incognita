"""Operations on time-ordered GPS point series: segment distance and speed."""

import pandas as pd

from incognita.geo_distance import get_haversine_dist


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
