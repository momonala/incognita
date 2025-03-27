import logging

import numpy as np
import pandas as pd

from incognita.utils import timed

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def get_haversine_dist(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Get true distance between two coordiantes (accounts for curvature of earth)."""
    radius_earth = 6378137  # Radius of earth in m
    delta_lat = lat2 * np.pi / 180 - lat1 * np.pi / 180
    delta_lon = lon2 * np.pi / 180 - lon1 * np.pi / 180
    a = np.sin(delta_lat / 2) * np.sin(delta_lat / 2) + np.cos(lat1 * np.pi / 180) * np.cos(
        lat2 * np.pi / 180
    ) * np.sin(delta_lon / 2) * np.sin(delta_lon / 2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    d = radius_earth * c
    return d


@timed
def add_speed_to_gdf(gdf: pd.DataFrame) -> pd.DataFrame:
    """Adds custom speed calc (dist/time) and reassigns index.

    Args:
        gdf: must have columns [lon, lat, timestamp]
    Returns:
        dataframe with additional calculated columns
    """
    gdf["time_diff"] = pd.to_datetime(gdf["timestamp"])
    diff = gdf["time_diff"].diff(1)
    gdf["time_diff"] = diff.astype(int) / 10**9  # convert to seconds
    haversine_args = gdf["lat"].shift(1), gdf["lon"].shift(1), gdf.loc[1:, 'lat'], gdf.loc[1:, 'lon']
    gdf['meters'] = get_haversine_dist(*haversine_args)
    gdf['speed_calc'] = gdf['meters'] / gdf['time_diff']
    gdf["index"] = gdf.index
    return gdf


@timed
def split_into_trips(gdf: pd.DataFrame, max_dist_meters: int = 400) -> pd.DataFrame:
    """Group indvidual coordinates into "trips" of arbitrary length.

    Args:
        gdf: must have columns [lon, lat]
        max_dist_meters: maximum distance between two points to consider them part of the same trip
            a value of 400 will show most data (including flight paths)
            a value of 100 will not show flight paths
    Returns:
        DataFrame where each row is a single "trip",
            as well as aggregations for that trip
    """
    min_points = 100  # trip must have at least a few points
    # indicies from gdf to split trips apart on
    indices_split_trips = list(gdf[gdf["meters"] > max_dist_meters].index)
    trips = []
    columns = ["geometry", "start", "end", "minutes", "n_points", "avg_m/s", "max_m/s", "min_m/s"]
    for i, stop_idx in enumerate(indices_split_trips + [gdf.index[-1]]):
        # get range of values within one trip
        start_idx = 0 if i == 0 else indices_split_trips[i - 1]
        trip_df = gdf.iloc[start_idx:stop_idx]

        # ensure they meet the logical conditions of a trip
        if trip_df.shape[0] <= min_points:
            continue
        trip_df = trip_df[trip_df["meters"] < max_dist_meters]  # remove far away points
        if trip_df.empty:
            continue

        # add to dataframe with aggregations
        geometry = trip_df[["lon", "lat"]].values.tolist()[::5]
        start = np.min(trip_df["timestamp"])
        stop = np.max(trip_df["timestamp"])
        duration_minutes = np.sum(trip_df["time_diff"]) / 60
        num_points = len(trip_df["timestamp"])
        avg_speed = np.mean(trip_df["speed_calc"])
        max_speed = np.max(trip_df["speed_calc"])
        min_speed = np.min(trip_df["speed_calc"])

        data = [[geometry, start, stop, duration_minutes, num_points, avg_speed, max_speed, min_speed]]
        linestring_gdf = pd.DataFrame(data, columns=columns)
        trips.append(linestring_gdf)

    trips_df = pd.concat(trips)
    trips_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    trips_df.dropna(subset=['avg_m/s'], inplace=True)
    trips_df.dropna(subset=['max_m/s'], inplace=True)

    return trips_df


@timed
def get_stationary_groups(
    gdf: pd.DataFrame, max_time_diff: int = 30, max_dist_meters: int = 10
) -> pd.DataFrame:
    """Returns a DataFrame where each row is a group of stationary points. Stationary is defined as
    points which stay within the `max_dist_meters` distance and within `max_time_diff` time. Applies
    aggregations to all relevant parent columns.
    """
    gdf = gdf.copy()  # prevent False positive set with copy warning
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
    stationary_groups.drop("timestamp", inplace=True, axis=1)

    return stationary_groups
