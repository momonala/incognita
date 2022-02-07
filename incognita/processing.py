import json
import logging
import sqlite3
from glob import glob
from typing import Union, Dict

import geopandas
import numpy as np
import pandas as pd
from shapely.geometry import LineString

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def get_processed_gdf_from_db() -> pd.DataFrame:
    """Returned the cached geojson/location dataframe."""
    conn = sqlite3.connect('geoData.db')
    return pd.read_sql('select * from overland', conn)


def write_gdf_to_db(gdf: geopandas.GeoDataFrame):
    """Write geojson/location dataframe to SQLite db."""
    conn = sqlite3.connect('geoData.db')
    gdf.to_sql('overland', conn, if_exists='replace', index=False)
    logger.info("Wrote table overland in geoData.db")


def _read_geojson_file(filename: str) -> Dict[Dict, str]:
    with open(filename) as f:
        return json.loads(f.read())["locations"]


def get_raw_gdf() -> pd.DataFrame:
    """Dump all raw json files in /raw_data into a GeoDataFrame. Only keep relevant keys."""
    geojson_files = glob("raw_data/*.geojson")
    data_json = sum([_read_geojson_file(f) for f in geojson_files], [])  # flatten
    data_json = sorted(data_json, key=lambda x: x["properties"]["timestamp"])

    parsed = []
    for d in data_json:
        parsed.append(
            {
                "lon": d["geometry"]["coordinates"][0],
                "lat": d["geometry"]["coordinates"][1],
                "timestamp": d["properties"]["timestamp"],
                "speed": d["properties"].get("speed"),
                "altitude": d["properties"].get("altitude"),
            }
        )
    gdf = pd.DataFrame(parsed)
    # gdf = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df.lon, df.lat))
    logger.info(f"{len(geojson_files)} files found")
    logger.info(f"created raw gdf with shape: {gdf.shape}")
    return gdf


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


def get_processed_gdf(
    gdf: Union[geopandas.GeoDataFrame, pd.DataFrame]
) -> Union[geopandas.GeoDataFrame, pd.DataFrame]:
    """Adds custom speed calc (dist/time) and reassigns index."""
    gdf["time_diff"] = pd.to_datetime(gdf.timestamp).diff(1).apply(lambda x: x.seconds)
    gdf['meters'] = get_haversine_dist(
        gdf.lat.shift(1), gdf.lon.shift(1), gdf.loc[1:, 'lat'], gdf.loc[1:, 'lon']
    )
    gdf['speed_calc'] = gdf['meters'] / gdf['time_diff']
    gdf["index"] = gdf.index
    return gdf


def split_into_trips(gdf: geopandas.GeoDataFrame, max_dist_meters: int = 400) -> geopandas.GeoDataFrame:
    """Group indvidual coordinates into "trips" of arbitrary length (LineStrings).

    Args:
        gdf: must have columns [lon, lat]
        max_dist_meters: maximum distance between two points to consider them part of the same trip
    Returns:
        GeoDataFrame with column trips of dtype Linestring, where each row is a single "trip"
    """
    min_points = 5  # LineString must have at least two points
    # indicies from gdf to split trips apart on
    indices_split_trips = list(gdf[gdf.meters > max_dist_meters].index)
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
        line = LineString(trip_df[["lon", "lat"]].values)
        line_gdf = geopandas.GeoDataFrame([line], columns=["geometry"])
        trips.append(line_gdf)

    trips = geopandas.GeoDataFrame(pd.concat(trips))
    return trips
