import json
import logging
import sqlite3
from functools import lru_cache

import pandas as pd

from incognita.utils import timed

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

DB_FILE = "data/geo_data.db"
DB_NAME = "overland"

MIN_HORIZONTAL_ACCURACY = 200.0


@timed
@lru_cache()
def get_gdf_from_db(db_filename: str = DB_FILE) -> pd.DataFrame:
    """Returned the cached geojson/location dataframe."""
    with sqlite3.connect(db_filename) as conn:
        df = pd.read_sql(f"select lon, lat, timestamp from {DB_NAME}", conn)
    return df.sort_values("timestamp").reset_index(drop=True)


def update_db(geojson_filename: str, db_filename: str = DB_FILE, conn: sqlite3.Connection | None = None):
    """Updates db: db_filename with contents of parsed geojson_filename

    Args:
        geojson_filename: Path to the GeoJSON file to process
        db_filename: Path to the database file (used if conn is None)
        conn: Optional existing database connection to reuse
    """
    raw_geojson = read_geojson_file(geojson_filename)
    if not raw_geojson:
        logger.info(f"Failed parsing file {geojson_filename}")
        return
    parsed = extract_properties_from_geojson(raw_geojson)
    df = pd.DataFrame(parsed)
    if df.empty:
        logger.info(f"No data to update db with {geojson_filename}")
        return

    if conn is not None:
        # Use provided connection
        df.to_sql(DB_NAME, conn, if_exists="append", index=False)
    else:
        # Create new connection
        with sqlite3.connect(db_filename) as conn:
            df.to_sql(DB_NAME, conn, if_exists="append", index=False)
    logger.info(f"Updated: {db_filename=} with: {geojson_filename=} size: {df.shape=}")


def create_timestamp_index():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(f"CREATE INDEX idx_timestamp ON {DB_NAME} (timestamp);")
        conn.commit()
    logger.info("Added index idx_timestamp")


def read_geojson_file(filename: str) -> list[dict] | None:
    """Return raw geojson entries as list of JSONs, plus source file name."""
    with open(filename) as f:
        try:
            data = f.read()
            raw_geojson = json.loads(data)["locations"]
            return [{**d, **{"geojson_file": filename}} for d in raw_geojson]
        except Exception as e:
            logger.error(f"Failed parsing {filename=} with {e}")
            return


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


@timed
def fetch_coordinates(
    lookback_hours: int = 24,
    min_accuracy: float | None = None,
) -> list[tuple]:
    """Return coordinates from the specified lookback period sorted by timestamp.

    Args:
        lookback_hours: number of hours to look back from current time (default: 24)
        age_db: timestamp of the oldest point in the database (default: get_last_timestamp())
        min_accuracy: minimum horizontal accuracy threshold in meters (default: None)
    Returns:
        List of (ISO 8601 timestamp, latitude, longitude, horizontal_accuracy) tuples from specified period,
        sorted by timestamp ascending
    """
    # Calculate exact timestamp bounds for the window we want
    end_time = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    start_time = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=lookback_hours)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        base_query = f"""
            SELECT 
                strftime('%Y-%m-%dT%H:%M:%SZ', timestamp) as timestamp,
                lat,
                lon,
                horizontal_accuracy
            FROM {DB_NAME}
            WHERE timestamp >= ? AND timestamp <= ?
            AND speed > 0
        """

        params = [start_time, end_time]
        if min_accuracy is not None:
            base_query += " AND horizontal_accuracy <= ?"
            params.append(min_accuracy)

        base_query += " ORDER BY timestamp ASC"
        cursor.execute(base_query, params)
        coordinates = cursor.fetchall()

        return [
            (ts, float(lat), float(lon), float(horizontal_accuracy))
            for ts, lat, lon, horizontal_accuracy in coordinates
        ]
