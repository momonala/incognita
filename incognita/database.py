import sqlite3

import geopandas
import pandas as pd

from incognita.processing import logger, get_processed_gdf, get_raw_gdf


def get_processed_gdf_from_db(query: str = 'select * from overland') -> pd.DataFrame:
    """Returned the cached geojson/location dataframe."""
    conn = sqlite3.connect('geoData.db')
    return pd.read_sql(query, conn)


def write_gdf_to_db(gdf: geopandas.GeoDataFrame):
    """Write geojson/location dataframe to SQLite db."""
    conn = sqlite3.connect('geoData.db')
    gdf.to_sql('overland', conn, if_exists='replace', index=False)
    logger.info("Wrote table overland in geoData.db")


def update_db():
    """Update the main db file."""
    write_gdf_to_db(get_processed_gdf(get_raw_gdf()))


def get_start_end_date():
    conn = sqlite3.connect('geoData.db')
    query = "select min(timestamp) as start_date, max(timestamp) as end_date from overland"
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchone()
