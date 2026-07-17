"""SQLite storage for raw HealthKit samples from the iOS export app."""

import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from incognita.data_models import HealthKitBatch, HealthKitExportType

logger = logging.getLogger(__name__)

HEALTH_DB_FILE = "data/health_data.db"
_HEALTH_DUMP_CACHE_TABLE = "daily_health_dump_cache"

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

    if inserted > 0:
        affected_dates = {sample.start[:10] for sample in batch.samples}
        invalidate_health_dump_cache(sorted(affected_dates), db_filename)

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


_HEALTH_DATE_FMT = "%Y-%m-%d"


def get_health_dump_for_date_range(
    start_date: str,
    end_date: str,
    db_filename: str = HEALTH_DB_FILE,
) -> dict:
    """Return daily and total HealthKit stats across an inclusive calendar date range."""
    start_dt = datetime.strptime(start_date, _HEALTH_DATE_FMT)
    end_dt = datetime.strptime(end_date, _HEALTH_DATE_FMT)
    if start_dt > end_dt:
        raise ValueError("start_date must be on or before end_date")

    days: list[dict] = []
    totals = {"steps": 0, "km": 0.0, "kcals": 0.0, "flights_climbed": 0}
    has_steps = has_km = has_kcals = has_flights = False

    current = start_dt
    while current <= end_dt:
        date_str = current.strftime(_HEALTH_DATE_FMT)
        daily = get_daily_health_dump(date_str, db_filename=db_filename)
        days.append({"date": date_str, **daily})

        if daily["steps"] is not None:
            totals["steps"] += int(daily["steps"])
            has_steps = True
        if daily["km"] is not None:
            totals["km"] += float(daily["km"])
            has_km = True
        if daily["kcals"] is not None:
            totals["kcals"] += float(daily["kcals"])
            has_kcals = True
        if daily["flights_climbed"] is not None:
            totals["flights_climbed"] += int(daily["flights_climbed"])
            has_flights = True
        current += timedelta(days=1)

    return {
        "totals": {
            "steps": totals["steps"] if has_steps else None,
            "km": round(totals["km"], 1) if has_km else None,
            "kcals": round(totals["kcals"]) if has_kcals else None,
            "flights_climbed": totals["flights_climbed"] if has_flights else None,
        },
        "days": days,
    }


def get_health_stats_for_date_range(
    start_date: str,
    end_date: str,
    db_filename: str = HEALTH_DB_FILE,
) -> dict[str, int | float | None]:
    """Sum steps and flights climbed across an inclusive calendar date range."""
    return get_health_dump_for_date_range(start_date, end_date, db_filename=db_filename)["totals"]


def get_health_dump_range(days: int, db_filename: str = HEALTH_DB_FILE) -> list[dict]:
    """Return dominant-hardware daily health totals for the last ``days`` days (oldest first).

    Past calendar days are served from a SQLite cache; today is always recomputed.
    """
    if days < 1:
        raise ValueError("days must be at least 1")

    today = datetime.now().date()
    today_str = today.strftime("%Y-%m-%d")
    dates = [(today - timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(days - 1, -1, -1)]

    rows_by_date: dict[str, dict] = {}
    to_compute: list[str] = []

    for date in dates:
        if date == today_str:
            to_compute.append(date)
            continue
        cached = _read_health_dump_cache(date, db_filename)
        if cached is not None:
            rows_by_date[date] = cached
        else:
            to_compute.append(date)

    for date in to_compute:
        payload = {"date": date, **get_daily_health_dump(date, db_filename=db_filename)}
        rows_by_date[date] = payload
        if date != today_str:
            _write_health_dump_cache(date, payload, db_filename)

    return [rows_by_date[date] for date in dates]


def invalidate_health_dump_cache(dates: list[str], db_filename: str = HEALTH_DB_FILE) -> None:
    """Drop cached health summaries for the given calendar days."""
    if not dates or not Path(db_filename).exists():
        return
    placeholders = ",".join("?" for _ in dates)
    with sqlite3.connect(db_filename) as conn:
        _ensure_health_dump_cache_table(conn)
        conn.execute(
            f"DELETE FROM {_HEALTH_DUMP_CACHE_TABLE} WHERE date IN ({placeholders})",
            dates,
        )
        conn.commit()


def _ensure_health_dump_cache_table(conn: sqlite3.Connection) -> None:
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {_HEALTH_DUMP_CACHE_TABLE} (
            date TEXT PRIMARY KEY,
            payload TEXT NOT NULL
        )
    """)


def _read_health_dump_cache(date: str, db_filename: str) -> dict | None:
    if not Path(db_filename).exists():
        return None
    with sqlite3.connect(db_filename) as conn:
        _ensure_health_dump_cache_table(conn)
        row = conn.execute(
            f"SELECT payload FROM {_HEALTH_DUMP_CACHE_TABLE} WHERE date = ?",
            (date,),
        ).fetchone()
    if row is None:
        return None
    return json.loads(row[0])


def _write_health_dump_cache(date: str, payload: dict, db_filename: str) -> None:
    with sqlite3.connect(db_filename) as conn:
        _ensure_health_dump_cache_table(conn)
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {_HEALTH_DUMP_CACHE_TABLE} (date, payload)
            VALUES (?, ?)
            """,
            (date, json.dumps(payload)),
        )
        conn.commit()


if __name__ == "__main__":
    init_db(HEALTH_DB_FILE)
