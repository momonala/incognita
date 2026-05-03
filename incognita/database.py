import json
import logging
import sqlite3
from functools import lru_cache

import pandas as pd

from incognita.observability import timed

logger = logging.getLogger(__name__)

DB_FILE = "data/geo_data.db"
DB_NAME = "overland"

MIN_HORIZONTAL_ACCURACY = 200.0


@timed
@lru_cache()
def get_gdf_from_db(
    db_filename: str = DB_FILE,
    date_min: str | None = None,
    date_max: str | None = None,
) -> pd.DataFrame:
    """Return the cached geojson/location dataframe, optionally filtered by timestamp range."""
    query = f"SELECT lon, lat, timestamp FROM {DB_NAME}"
    params: list = []
    if date_min is not None and date_max is not None:
        query += " WHERE timestamp >= ? AND timestamp <= ?"
        params = [date_min, date_max]
    query += " ORDER BY timestamp"
    with sqlite3.connect(db_filename) as conn:
        df = pd.read_sql(query, conn, params=params if params else None)
    return df.reset_index(drop=True)


def update_db(
    geojson_filename: str,
    db_filename: str = DB_FILE,
    conn: sqlite3.Connection | None = None,
) -> None:
    """Update database with contents of parsed GeoJSON file.

    geojson_filename is appended to the table; conn is used if provided, else a new connection to db_filename.
    """
    raw_geojson = read_geojson_file(geojson_filename)
    if not raw_geojson:
        logger.warning("Failed parsing file %s", geojson_filename)
        return
    parsed = extract_properties_from_geojson(raw_geojson)
    df = pd.DataFrame(parsed)
    if df.empty:
        logger.warning("No data to update db with %s", geojson_filename)
        return

    if conn is not None:
        # Use provided connection
        df.to_sql(DB_NAME, conn, if_exists="append", index=False)
    else:
        # Create new connection
        with sqlite3.connect(db_filename) as conn:
            df.to_sql(DB_NAME, conn, if_exists="append", index=False)
    logger.debug(
        "Updated db_filename=%s with geojson_filename=%s shape=%s", db_filename, geojson_filename, df.shape
    )


def create_timestamp_index() -> None:
    """Create timestamp index on the location table for faster range queries."""
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(f"CREATE INDEX idx_timestamp ON {DB_NAME} (timestamp);")
        conn.commit()
    logger.info("Added index idx_timestamp")


def read_geojson_file(filename: str) -> list[dict] | None:
    """Return parsed GeoJSON location entries with source filename added, or None on parse error."""
    with open(filename, encoding="utf-8") as f:
        try:
            data = json.load(f)
            raw_geojson = data["locations"]
            return [{**d, "geojson_file": filename} for d in raw_geojson]
        except (KeyError, json.JSONDecodeError) as e:
            logger.error("Failed parsing %s: %s", filename, e)
            return None


def filter_by_accuracy(geo_data: list[dict], min_horizontal_accuracy: float) -> list[dict]:
    """Filter out GPS points with horizontal accuracy worse than specified threshold."""
    return [
        point
        for point in geo_data
        if point["properties"].get("horizontal_accuracy", float("inf")) <= min_horizontal_accuracy
    ]


def extract_properties_from_geojson(
    geo_data: list[dict], min_horizontal_accuracy: float = MIN_HORIZONTAL_ACCURACY
) -> list[dict]:
    """Parse out the relevant content from a raw geojson file."""
    geo_data = filter_by_accuracy(geo_data, min_horizontal_accuracy)

    geo_data_parsed = []
    for d in geo_data:
        try:
            motion = d["properties"].get("motion", [])
            motion = motion[0] if motion else None
            geo_data_parsed.append(
                {
                    "lon": d["geometry"]["coordinates"][0],
                    "lat": d["geometry"]["coordinates"][1],
                    "timestamp": d["properties"]["timestamp"],
                    "speed": d["properties"].get("speed"),
                    "altitude": d["properties"].get("altitude"),
                    "horizontal_accuracy": d["properties"].get("horizontal_accuracy"),
                    "motion": motion,
                    "geojson_file": d["geojson_file"],
                }
            )
        except KeyError:
            logger.exception(f"ERROR skipping row {d}")
            continue

    return geo_data_parsed


def get_gdf_for_map(
    date_min: str,
    date_max: str,
    min_accuracy: float = 100.0,
    db_filename: str = DB_FILE,
) -> pd.DataFrame:
    """Return GPS points for the DB-backed /coordinates API.

    Only moving points (speed > 0) with horizontal_accuracy <= min_accuracy
    in the given timestamp range.

    Returns:
        DataFrame with columns lon, lat, timestamp, accuracy, speed.
    """
    with sqlite3.connect(db_filename) as conn:
        query = f"""
            SELECT lon, lat, timestamp, horizontal_accuracy AS accuracy, speed
            FROM {DB_NAME}
            WHERE timestamp >= ? AND timestamp <= ?
            AND speed > 0
            AND horizontal_accuracy <= ?
            ORDER BY timestamp ASC
        """
        df = pd.read_sql(query, conn, params=[date_min, date_max, min_accuracy])
    return df.reset_index(drop=True)
