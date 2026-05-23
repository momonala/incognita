"""SQLite storage for raw HealthKit samples from the iOS export app."""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

from incognita.data_models import HealthKitBatch, HealthKitExportType

logger = logging.getLogger(__name__)

HEALTH_DB_FILE = "data/health_data.db"

_CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS {table} (
        uuid TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        start TEXT NOT NULL,
        end TEXT NOT NULL,
        value REAL,
        unit TEXT,
        source TEXT,
        device_name TEXT,
        device_model TEXT,
        device_manufacturer TEXT,
        device_hardware_version TEXT,
        device_software_version TEXT,
        metadata TEXT,
        batch_index INTEGER NOT NULL
    )
"""

_INSERT_SQL = """
    INSERT OR IGNORE INTO {table} (
        uuid, type, start, end, value, unit, source,
        device_name, device_model, device_manufacturer,
        device_hardware_version, device_software_version,
        metadata, batch_index
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def init_db(db_filename: str = HEALTH_DB_FILE) -> None:
    """Create the health database and all HealthKit tables if they don't exist.

    Safe to call repeatedly. Called automatically by insert_health_batch on first use,
    but can also be run directly: python -m incognita.health_database
    """
    Path(db_filename).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_filename) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        for export_type in HealthKitExportType:
            conn.execute(_CREATE_TABLE_SQL.format(table=export_type.table_name))
        conn.commit()
    logger.info("✅ Health DB initialised at %s", db_filename)


def insert_health_batch(
    batch: HealthKitBatch,
    db_filename: str = HEALTH_DB_FILE,
) -> tuple[int, int]:
    """Insert validated samples into per-type tables. Returns (inserted, skipped)."""
    Path(db_filename).parent.mkdir(parents=True, exist_ok=True)

    rows_by_table: dict[str, list[tuple]] = {
        export_type.table_name: [] for export_type in HealthKitExportType
    }
    for sample in batch.samples:
        rows_by_table[sample.type.table_name].append(sample.sqlite_row(batch.batch_index))

    inserted = 0
    with sqlite3.connect(db_filename) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        for export_type in HealthKitExportType:
            conn.execute(_CREATE_TABLE_SQL.format(table=export_type.table_name))

        for table_name, rows in rows_by_table.items():
            if not rows:
                continue
            before = conn.total_changes
            conn.executemany(_INSERT_SQL.format(table=table_name), rows)
            inserted += conn.total_changes - before
        conn.commit()

    skipped = len(batch.samples) - inserted
    logger.debug(
        "health_db batch_index=%s inserted=%s skipped=%s db_filename=%s",
        batch.batch_index,
        inserted,
        skipped,
        db_filename,
    )
    return inserted, skipped


def filter_dominant_hardware_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Filter raw sample rows to only those from the dominant hardware per day.

    Requires df to have a 'day' (str) and 'device_hardware_version' column.
    For each day the hardware version with the most rows is kept; ties are broken
    alphabetically so the result is deterministic.
    """
    if df.empty:
        return df
    counts = (
        df.fillna({"device_hardware_version": "(unknown)"})
        .groupby(["day", "device_hardware_version"], as_index=False)
        .size()
        .rename(columns={"size": "_count"})
    )
    dominant = (
        counts.sort_values("device_hardware_version")
        .loc[counts.groupby("day")["_count"].transform("max") == counts["_count"]]
        .drop_duplicates(subset="day", keep="first")[["day", "device_hardware_version"]]
    )
    return df.fillna({"device_hardware_version": "(unknown)"}).merge(
        dominant, on=["day", "device_hardware_version"], how="inner"
    )


def load_metric_df(
    export_type: HealthKitExportType,
    db_filename: str = HEALTH_DB_FILE,
    date_from: str | None = None,
    date_to: str | None = None,
    devices: list[str] | None = None,
) -> pd.DataFrame:
    """Load raw samples for one metric type with optional filters.

    Args:
        export_type: Which HealthKit metric table to query.
        db_filename: Path to the SQLite database.
        date_from: ISO date string (YYYY-MM-DD) lower bound on sample start time, inclusive.
        date_to: ISO date string (YYYY-MM-DD) upper bound on sample start time, inclusive.
        devices: Restrict to these device names; None means all devices.

    Returns:
        DataFrame with columns uuid, start, end, value, unit, source, device_name.
    """
    if not Path(db_filename).exists():
        return pd.DataFrame()

    conditions: list[str] = []
    params: list = []
    if date_from:
        conditions.append("start >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("start <= ?")
        params.append(date_to + "T23:59:59Z")
    if devices:
        placeholders = ",".join("?" * len(devices))
        conditions.append(f"device_name IN ({placeholders})")
        params.extend(devices)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT uuid, start, end, value, unit, source, device_name, device_hardware_version FROM {export_type.table_name} {where} ORDER BY start"

    with sqlite3.connect(db_filename) as conn:
        return pd.read_sql(sql, conn, params=params or None)


def get_health_meta(db_filename: str = HEALTH_DB_FILE) -> dict:
    """Return devices and date range available across all metric tables.

    Returns:
        Dict with keys 'devices' (sorted list of device name strings) and
        'date_range' (dict with 'min'/'max' ISO date strings, or None if empty).
    """
    if not Path(db_filename).exists():
        return {"devices": [], "date_range": {"min": None, "max": None}}

    all_devices: set[str] = set()
    min_date: str | None = None
    max_date: str | None = None

    with sqlite3.connect(db_filename) as conn:
        for export_type in HealthKitExportType:
            table = export_type.table_name
            try:
                for (device,) in conn.execute(
                    f"SELECT DISTINCT device_name FROM {table} WHERE device_name IS NOT NULL"
                ):
                    all_devices.add(device)
                row = conn.execute(f"SELECT MIN(start), MAX(start) FROM {table}").fetchone()
                if row and row[0]:
                    if min_date is None or row[0] < min_date:
                        min_date = row[0]
                    if max_date is None or row[1] > max_date:
                        max_date = row[1]
            except sqlite3.OperationalError:
                pass

    return {
        "devices": sorted(all_devices),
        "date_range": {
            "min": min_date[:10] if min_date else None,
            "max": max_date[:10] if max_date else None,
        },
    }


def get_daily_health_dump(date: str, db_filename: str = HEALTH_DB_FILE) -> dict:
    """Return dominant-hardware daily totals for all metrics on a given date.

    Args:
        date: ISO date string (YYYY-MM-DD) to aggregate.
        db_filename: Path to the SQLite database.

    Returns:
        Dict with keys steps, kcals, km, flights_climbed (all nullable).
    """
    _METRIC_AGG: dict[str, tuple[str, float]] = {
        # table_name → (pandas agg fn, unit scale factor)
        "step_count": ("sum", 1.0),
        "active_energy": ("sum", 1.0),
        "distance": ("sum", 0.001),  # m → km
        "flights_climbed": ("sum", 1.0),
    }
    _METRIC_KEY: dict[str, str] = {
        "step_count": "steps",
        "active_energy": "kcals",
        "distance": "km",
        "flights_climbed": "flights_climbed",
    }

    result: dict[str, float | None] = {v: None for v in _METRIC_KEY.values()}

    if not Path(db_filename).exists():
        return result

    for export_type in HealthKitExportType:
        metric = export_type.table_name
        agg_fn, scale = _METRIC_AGG[metric]
        sql = f"SELECT uuid, value, device_hardware_version FROM {metric} " "WHERE start >= ? AND start <= ?"
        with sqlite3.connect(db_filename) as conn:
            df = pd.read_sql(sql, conn, params=[date, date + "T23:59:59Z"])

        if df.empty:
            continue

        df["day"] = date
        df = filter_dominant_hardware_rows(df)
        total = float(df["value"].sum() if agg_fn == "sum" else df["value"].mean())
        result[_METRIC_KEY[metric]] = round(total * scale, 2)

    return result


if __name__ == "__main__":
    init_db(HEALTH_DB_FILE)
