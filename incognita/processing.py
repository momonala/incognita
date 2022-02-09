import json
import logging
from glob import glob
from typing import Union, Dict, List

import numpy as np
import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import LineString

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def read_geojson_file(filename: str) -> List[Dict]:
    """Return raw geojson entries as list of JSONs, plus source file name. """
    with open(filename) as f:
        raw_geojson = json.loads(f.read())["locations"]
        return [{**d, **{"geojson_file": filename}} for d in raw_geojson]


def extract_properties_from_geojson(geo_data: List[Dict]) -> List[Dict[str, Union[str, int]]]:
    """Parse out the relevant contents from a raw geojson file."""
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


def get_raw_gdf() -> pd.DataFrame:
    """Dump ALL raw json files in /raw_data into a GeoDataFrame. Only keep relevant keys."""
    geojson_files = glob("raw_data/*.geojson")
    data_json = sum(
        [extract_properties_from_geojson(read_geojson_file(f)) for f in geojson_files], []
    )  # flatten
    parsed = sorted(data_json, key=lambda x: x["timestamp"])
    raw_geojson_df = pd.DataFrame(parsed)
    # gdf = GeoDataFrame(df, geometry=points_from_xy(df.lon, df.lat))  # if we want a GeoDataFrame instead
    logger.info(f"{len(geojson_files)} files found")
    logger.info(f"created: {raw_geojson_df.shape=}")
    return raw_geojson_df


def get_haversine_dist(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Get true distance between to coordiantes."""
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
    gdf["time_diff"] = pd.to_datetime(gdf["timestamp"]).diff(1).apply(lambda x: x.seconds)
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
    Returns:
        GeoDataFrame with column trips of dtype Linestring, where each row is a single "trip"
    """
    min_points = 5  # LineString must have at least two points
    # indicies from gdf to split trips apart on
    indices_split_trips = list(gdf[gdf["meters"] > max_dist_meters].index)
    trips = []
    for i, stop_idx in enumerate(indices_split_trips + [gdf.index[-1]]):
        # get range of values within one trip
        start_idx = 0 if i == 0 else indices_split_trips[i - 1]
        trip_df = gdf.loc[start_idx:stop_idx]

        # ensure they meet the logical conditions of a trip
        if trip_df.shape[0] <= min_points:
            continue
        trip_df = trip_df[trip_df.meters < max_dist_meters]  # remove far away points
        if trip_df.empty:
            continue

        # add to dataframe
        line_string = LineString(trip_df[["lon", "lat"]].values)
        linestring_gdf = GeoDataFrame([line_string], columns=["geometry"])
        trips.append(linestring_gdf)

    trips = GeoDataFrame(pd.concat(trips))
    return trips
