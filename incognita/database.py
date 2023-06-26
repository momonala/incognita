import json
import logging
import sqlite3
from functools import lru_cache
import pandas as pd
from incognita.utils import timed

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

DB_FILE = "cache/geo_data.db"


@timed
@lru_cache()
def get_gdf_from_db(db_filename: str = DB_FILE) -> pd.DataFrame:
    """Returned the cached geojson/location dataframe."""
    with sqlite3.connect(db_filename) as conn:
        df = pd.read_sql('select lon, lat, timestamp from overland', conn)
    return df.sort_values("timestamp").reset_index(drop=True)


def update_db(geojson_filename: str, db_filename: str = DB_FILE):
    """Updates db: db_filename with contents of parsed geojson_filename"""
    raw_geojson = read_geojson_file(geojson_filename)
    parsed = extract_properties_from_geojson(raw_geojson)
    df = pd.DataFrame(parsed)

    with sqlite3.connect(db_filename) as conn:
        df.to_sql('overland', conn, if_exists='append', index=False)
    logger.info(f"Updated: {db_filename=} with: {geojson_filename=} size: {df.shape=}")


@lru_cache()
def get_start_end_date(db_filename: str = DB_FILE) -> tuple[str, str]:
    """Returns the first and last timestamps in the db"""
    query = "select min(timestamp) as start_date, max(timestamp) as end_date from overland"
    with sqlite3.connect(db_filename) as conn:
        cur = conn.cursor()
        cur.execute(query)
        return cur.fetchone()


def read_geojson_file(filename: str) -> list[dict]:
    """Return raw geojson entries as list of JSONs, plus source file name."""
    with open(filename) as f:
        raw_geojson = json.loads(f.read())["locations"]
        return [{**d, **{"geojson_file": filename}} for d in raw_geojson]


def extract_properties_from_geojson(geo_data: list[dict]) -> list[dict]:
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
