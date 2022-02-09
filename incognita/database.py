import logging
import sqlite3
from typing import Tuple

import pandas as pd
from geopandas import GeoDataFrame

from incognita.processing import read_geojson_file, extract_properties_from_geojson

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

DB_FILE = "cache/geo_data.db"


def get_gdf_from_db(db_filename: str = DB_FILE) -> pd.DataFrame:
    """Returned the cached geojson/location dataframe."""
    with sqlite3.connect(db_filename) as conn:
        df = pd.read_sql('select * from overland', conn)
    return df.sort_values("timestamp").reset_index(drop=True)


def write_gdf_to_db(gdf: GeoDataFrame, db_filename: str):
    """Write geojson/location dataframe to SQLite db. Raises a ValueError if table already exists."""
    with sqlite3.connect(db_filename) as conn:
        gdf.to_sql('overland', conn, if_exists='fail', index=False)
    logger.info(f"wrote: {db_filename=}")


def update_db(geojson_filename: str, db_filename: str = DB_FILE):
    """Updates db: db_filename with contents of parsed geojson_filename"""
    raw_geojson = read_geojson_file(geojson_filename)
    parsed = extract_properties_from_geojson(raw_geojson)
    df = pd.DataFrame(parsed)

    with sqlite3.connect(db_filename) as conn:
        df.to_sql('overland', conn, if_exists='append', index=False)
    logger.info(f"Updated: {db_filename=} with: {geojson_filename=} size: {df.shape=}")


def get_start_end_date(db_filename: str = DB_FILE) -> Tuple[str, str]:
    """Returns the first and last timestamps in the db"""
    query = "select min(timestamp) as start_date, max(timestamp) as end_date from overland"
    with sqlite3.connect(db_filename) as conn:
        cur = conn.cursor()
        cur.execute(query)
        return cur.fetchone()
