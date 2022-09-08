import json
import logging
from typing import Union, Dict, List

import numpy as np
import pandas as pd
from geopandas import GeoDataFrame, points_from_xy
from shapely.geometry import LineString, Point

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def convert_pd_to_gpd(df: pd.DataFrame) -> GeoDataFrame:
    return GeoDataFrame(df, geometry=points_from_xy(df.lon, df.lat))


def read_geojson_file(filename: str) -> List[Dict]:
    """Return raw geojson entries as list of JSONs, plus source file name. """
    with open(filename) as f:
        raw_geojson = json.loads(f.read())["locations"]
        return [{**d, **{"geojson_file": filename}} for d in raw_geojson]


def extract_properties_from_geojson(geo_data: List[Dict]) -> List[Dict[str, Union[str, int]]]:
    """Parse out the relevant content from a raw geojson file."""
    return [
        {
            "lon": d["geometry"]["coordinates"][0],
            "lat": d["geometry"]["coordinates"][1],
            "timestamp": d["properties"]["timestamp"],
            "speed": d["properties"].get("speed"),
            "altitude": d["properties"].get("altitude"),
            "geojson_file": d["geojson_file"],
        }
        for d in geo_data
    ]


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


def add_speed_to_gdf(gdf: Union[GeoDataFrame, pd.DataFrame]) -> Union[GeoDataFrame, pd.DataFrame]:
    """Adds custom speed calc (dist/time) and reassigns index.

    Args:
        gdf: must have columns [lon, lat, timestamp]
    Returns:
        dataframe with additional calculated columns
    """
    gdf["time_diff"] = pd.to_datetime(gdf["timestamp"])
    diff = gdf["time_diff"].diff(1)
    gdf["time_diff"] = diff.view(int) / 10 ** 9  # convert to seconds
    gdf['meters'] = get_haversine_dist(
        gdf["lat"].shift(1), gdf["lon"].shift(1), gdf.loc[1:, 'lat'], gdf.loc[1:, 'lon']
    )
    gdf['speed_calc'] = gdf['meters'] / gdf['time_diff']
    gdf["index"] = gdf.index
    return gdf


def split_into_trips(gdf: Union[GeoDataFrame, pd.DataFrame], max_dist_meters: int = 400) -> GeoDataFrame:
    """Group indvidual coordinates into "trips" of arbitrary length (LineStrings).

    Args:
        gdf: must have columns [lon, lat]
        max_dist_meters: maximum distance between two points to consider them part of the same trip
            a value of 400 will show most data (including flight paths)
            a value of 100 will not show flight paths
    Returns:
        GeoDataFrame with column geometry of dtype LineString, where each row is a single "trip",
            as well as aggregations for that trip
    """
    min_points = 5  # LineString must have at least a few points
    # indicies from gdf to split trips apart on
    indices_split_trips = list(gdf[gdf["meters"] > max_dist_meters].index)
    trips = []
    columns = ["geometry", "start", "stop", "minutes", "n_points", "avg_m/s", "max_m/s", "min_m/s"]
    for i, stop_idx in enumerate(indices_split_trips + [gdf.index[-1]]):
        # get range of values within one trip
        start_idx = 0 if i == 0 else indices_split_trips[i - 1]
        trip_df = gdf.loc[start_idx:stop_idx]

        # ensure they meet the logical conditions of a trip
        if trip_df.shape[0] <= min_points:
            continue
        trip_df = trip_df[trip_df["meters"] < max_dist_meters]  # remove far away points
        if trip_df.empty:
            continue

        # add to dataframe with aggregations
        line_string = LineString(trip_df[["lon", "lat"]].values)
        start = str(np.min(trip_df["timestamp"]))
        stop = str(np.max(trip_df["timestamp"]))
        duration_minutes = np.sum(trip_df["time_diff"]) / 60
        num_points = len(trip_df["timestamp"])
        avg_speed = np.mean(trip_df["speed_calc"])
        max_speed = np.max(trip_df["speed_calc"])
        min_speed = np.min(trip_df["speed_calc"])

        data = [[line_string, start, stop, duration_minutes, num_points, avg_speed, max_speed, min_speed]]
        linestring_gdf = GeoDataFrame(data, columns=columns)
        trips.append(linestring_gdf)

    trips_df = GeoDataFrame(pd.concat(trips))
    return trips_df


def get_stationary_groups(
    gdf: Union[GeoDataFrame, pd.DataFrame], max_time_diff: int = 30, max_dist_meters: int = 10
) -> GeoDataFrame:
    """Returns a GeoDataFrame where each row is a group of stationary points. Stationary is defined as
    points which stay within the `max_dist_meters` distance and within `max_time_diff` time. Applies
    aggregations to all relevant parent columns.
    """
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
    stationary_groups["start"] = stationary_groups["timestamp"].apply(min).astype(str)
    stationary_groups["end"] = stationary_groups["timestamp"].apply(max).astype(str)
    stationary_groups["num_points"] = stationary_groups["lat"].apply(len)
    stationary_groups["lat"] = stationary_groups["lat"].apply(np.mean)
    stationary_groups["lon"] = stationary_groups["lon"].apply(np.mean)
    stationary_groups["geometry"] = stationary_groups.apply(lambda x: Point(x.lon, x.lat), axis=1)
    if "altitude" in stationary_groups.columns:
        stationary_groups["altitude"] = stationary_groups["altitude"].apply(lambda x: np.mean(np.array(x)))
    stationary_groups.drop("timestamp", inplace=True, axis=1)

    return GeoDataFrame(stationary_groups)
