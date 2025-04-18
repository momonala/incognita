import json
import logging
import sqlite3
from functools import lru_cache

import pandas as pd

from incognita.utils import timed

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

DB_FILE = "cache/geo_data.db"
DB_NAME = "overland"


@timed
@lru_cache()
def get_gdf_from_db(db_filename: str = DB_FILE) -> pd.DataFrame:
    """Returned the cached geojson/location dataframe."""
    with sqlite3.connect(db_filename) as conn:
        df = pd.read_sql(f'select lon, lat, timestamp from {DB_NAME}', conn)
    return df.sort_values("timestamp").reset_index(drop=True)


def update_db(geojson_filename: str, db_filename: str = DB_FILE):
    """Updates db: db_filename with contents of parsed geojson_filename"""
    raw_geojson = read_geojson_file(geojson_filename)
    if not raw_geojson:
        logger.info(f"Failed parsing file {geojson_filename}")
        return
    parsed = extract_properties_from_geojson(raw_geojson)
    df = pd.DataFrame(parsed)

    with sqlite3.connect(db_filename) as conn:
        df.to_sql(DB_NAME, conn, if_exists='append', index=False)
    logger.info(f"Updated: {db_filename=} with: {geojson_filename=} size: {df.shape=}")


def create_timestamp_index():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(f'CREATE INDEX idx_timestamp ON {DB_NAME} (timestamp);')
        conn.commit()
    logger.info(f"Added index idx_timestamp")


def read_geojson_file(filename: str) -> list[dict] | None:
    """Return raw geojson entries as list of JSONs, plus source file name."""
    with open(filename) as f:
        try:
            data = f.read()
        except Exception as e:
            logger.error(f"Failed parsing {filename=} with {e}")
            return
        raw_geojson = json.loads(data)["locations"]
        return [{**d, **{"geojson_file": filename}} for d in raw_geojson]


def extract_properties_from_geojson(geo_data: list[dict]) -> list[dict]:
    """Parse out the relevant content from a raw geojson file."""
    geo_data_parsed = []
    for d in geo_data:
        try:
            geo_data_parsed.append(
                {
                    "lon": d["geometry"]["coordinates"][0],
                    "lat": d["geometry"]["coordinates"][1],
                    "timestamp": d["properties"]["timestamp"],
                    "speed": d["properties"].get("speed"),
                    "altitude": d["properties"].get("altitude"),
                    "geojson_file": d["geojson_file"],
                }
            )
        except KeyError:
            print(f"ERROR skipping row {d}")
            continue

    return geo_data_parsed


@timed
def get_recent_coordinates() -> list[tuple]:
    """Return coordinates from the last 24 hours sorted by timestamp.
    
    Args:
        limit: maximum number of coordinates to return as a safeguard
    Returns:
        List of (ISO 8601 timestamp, latitude, longitude) tuples from last 24 hours,
        sorted by timestamp ascending
    """
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        query = f"""
            SELECT 
                strftime('%Y-%m-%dT%H:%M:%SZ', timestamp) as timestamp,
                lat,
                lon 
            FROM {DB_NAME}
            WHERE timestamp >= datetime('now', '-1 day')
            ORDER BY timestamp ASC
        """
        cursor.execute(query)
        coordinates = cursor.fetchall()
        
        return [(ts, float(lat), float(lon)) for ts, lat, lon in coordinates]
