"""Daily GPS motion statistics from the Overland SQLite database."""

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from incognita.database import DB_FILE, DB_NAME
from incognita.gps_geometry import add_speed_to_gdf

MOTION_CATEGORIES = ("automotive", "cycling", "running", "stationary", "unknown", "walking")

ALTITUDE_SAMPLE_EVERY_N = 60
_MOTION_STATS_CACHE_TABLE = "daily_motion_stats_cache"

_DAILY_POINTS_SQL = f"""
SELECT lon, lat, timestamp, speed, altitude, motion
FROM {DB_NAME}
WHERE timestamp >= ? AND timestamp <= ?
ORDER BY timestamp ASC
"""

_POINT_COLUMNS = ["lon", "lat", "timestamp", "speed", "altitude", "motion"]


def get_daily_motion_stats(date: str, db_filename: str = DB_FILE) -> dict:
    """Return daily motion stats for a calendar day (YYYY-MM-DD)."""
    db_path = Path(db_filename)
    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {db_filename}")

    day_points = _load_daily_points(date, db_filename)
    return _aggregate_motion_stats(day_points, date)


def get_motion_stats_range(days: int, db_filename: str = DB_FILE) -> list[dict]:
    """Return daily motion stats for the last ``days`` calendar days ending today (oldest first).

    Past calendar days are served from a SQLite cache; today is always recomputed.
    """
    if days < 1:
        raise ValueError("days must be at least 1")

    today = datetime.now().date()
    today_str = today.strftime("%Y-%m-%d")
    dates = [(today - timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(days - 1, -1, -1)]

    stats_by_date: dict[str, dict] = {}
    to_compute: list[str] = []

    for date in dates:
        if date == today_str:
            to_compute.append(date)
            continue
        cached = _read_motion_stats_cache(date, db_filename)
        if cached is not None:
            stats_by_date[date] = cached
        else:
            to_compute.append(date)

    for date, stats in _compute_motion_stats_for_dates(to_compute, db_filename).items():
        stats_by_date[date] = stats
        if date != today_str:
            _write_motion_stats_cache(date, stats, db_filename)

    return [stats_by_date[date] for date in dates]


def invalidate_motion_stats_cache(dates: list[str], db_filename: str = DB_FILE) -> None:
    """Drop cached motion stats for the given calendar days (e.g. after new GPS uploads)."""
    if not dates or not Path(db_filename).exists():
        return
    placeholders = ",".join("?" for _ in dates)
    with sqlite3.connect(db_filename) as conn:
        _ensure_motion_stats_cache_table(conn)
        conn.execute(
            f"DELETE FROM {_MOTION_STATS_CACHE_TABLE} WHERE date IN ({placeholders})",
            dates,
        )
        conn.commit()


def _compute_motion_stats_for_dates(dates: list[str], db_filename: str) -> dict[str, dict]:
    if not dates:
        return {}
    if len(dates) == 1:
        return {dates[0]: get_daily_motion_stats(dates[0], db_filename=db_filename)}

    points = _load_range_points(dates[0], dates[-1], db_filename)
    if points.empty:
        return {date: _empty_motion_stats(date) for date in dates}

    points["day"] = pd.to_datetime(points["timestamp"], utc=True).dt.strftime("%Y-%m-%d")
    return {
        date: _aggregate_motion_stats(
            points.loc[points["day"] == date, _POINT_COLUMNS].reset_index(drop=True),
            date,
        )
        for date in dates
    }


def _ensure_motion_stats_cache_table(conn: sqlite3.Connection) -> None:
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {_MOTION_STATS_CACHE_TABLE} (
            date TEXT PRIMARY KEY,
            payload TEXT NOT NULL
        )
    """)


def _read_motion_stats_cache(date: str, db_filename: str) -> dict | None:
    if not Path(db_filename).exists():
        return None
    with sqlite3.connect(db_filename) as conn:
        _ensure_motion_stats_cache_table(conn)
        row = conn.execute(
            f"SELECT payload FROM {_MOTION_STATS_CACHE_TABLE} WHERE date = ?",
            (date,),
        ).fetchone()
    if row is None:
        return None
    return json.loads(row[0])


def _write_motion_stats_cache(date: str, stats: dict, db_filename: str) -> None:
    with sqlite3.connect(db_filename) as conn:
        _ensure_motion_stats_cache_table(conn)
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {_MOTION_STATS_CACHE_TABLE} (date, payload)
            VALUES (?, ?)
            """,
            (date, json.dumps(stats)),
        )
        conn.commit()


def _load_daily_points(date: str, db_filename: str) -> pd.DataFrame:
    return _load_range_points(date, date, db_filename)


def _load_range_points(date_min: str, date_max: str, db_filename: str) -> pd.DataFrame:
    date_max_ts = f"{date_max}T23:59:59Z"
    with sqlite3.connect(db_filename) as conn:
        return pd.read_sql(_DAILY_POINTS_SQL, conn, params=[date_min, date_max_ts])


def _aggregate_motion_stats(day_points: pd.DataFrame, date: str) -> dict:
    if day_points.empty:
        return _empty_motion_stats(date)

    motion_type = _empty_motion_type()

    moving = day_points.loc[_is_moving(day_points)].copy()
    if moving.empty:
        motion_type["stationary"]["time_seconds"] = round(_stationary_seconds(day_points), 1)
        return _build_stats_dict(date, motion_type)

    moving = add_speed_to_gdf(moving)
    segments = moving.loc[moving["meters"].fillna(0) > 0]
    speeds = moving["speed"].dropna()
    altitude_ascended_m, altitude_descended_m = _altitude_ascended_descended(moving["altitude"])

    if not segments.empty:
        _apply_segment_totals_by_motion(segments, motion_type)

    motion_type["stationary"]["time_seconds"] = round(_stationary_seconds(day_points), 1)

    return _build_stats_dict(
        date,
        motion_type,
        total_km=float(segments["meters"].sum() / 1000.0) if not segments.empty else 0.0,
        time_spent_seconds=float(segments["time_diff"].sum()) if not segments.empty else 0.0,
        max_speed_m_s=float(speeds.max()) if not speeds.empty else 0.0,
        avg_speed_m_s=float(speeds.mean()) if not speeds.empty else 0.0,
        altitude_ascended_m=altitude_ascended_m,
        altitude_descended_m=altitude_descended_m,
    )


def _empty_motion_stats(date: str) -> dict:
    return _build_stats_dict(date, _empty_motion_type())


def _empty_motion_type() -> dict[str, dict[str, float]]:
    return {category: {"distance_km": 0.0, "time_seconds": 0.0} for category in MOTION_CATEGORIES}


def _build_stats_dict(
    date: str,
    motion_type: dict[str, dict[str, float]],
    *,
    total_km: float = 0.0,
    time_spent_seconds: float = 0.0,
    max_speed_m_s: float = 0.0,
    avg_speed_m_s: float = 0.0,
    altitude_ascended_m: float = 0.0,
    altitude_descended_m: float = 0.0,
) -> dict:
    return {
        "date": date,
        "total_km": round(total_km, 3),
        "max_speed_m_s": round(max_speed_m_s, 3),
        "avg_speed_m_s": round(avg_speed_m_s, 3),
        "time_spent_seconds": round(time_spent_seconds, 1),
        "altitude_ascended_m": round(altitude_ascended_m, 1),
        "altitude_descended_m": round(altitude_descended_m, 1),
        "motion_type": motion_type,
    }


def _is_moving(day_points: pd.DataFrame) -> pd.Series:
    return day_points["speed"].fillna(0) > 0


def _stationary_seconds(day_points: pd.DataFrame) -> float:
    """Seconds labeled stationary from the full day (includes speed=0 rows)."""
    timeline = add_speed_to_gdf(day_points.copy())
    return float(timeline.loc[timeline["motion"] == "stationary", "time_diff"].fillna(0).sum())


def _apply_segment_totals_by_motion(
    segments: pd.DataFrame,
    motion_type: dict[str, dict[str, float]],
) -> None:
    grouped = segments.groupby("motion", as_index=False).agg(
        distance_km=("meters", lambda values: values.sum() / 1000.0),
        time_seconds=("time_diff", "sum"),
    )
    for row in grouped.itertuples(index=False):
        if row.motion not in motion_type or row.motion == "stationary":
            continue
        motion_type[row.motion] = {
            "distance_km": round(float(row.distance_km), 3),
            "time_seconds": round(float(row.time_seconds), 1),
        }


def _altitude_ascended_descended(altitudes: pd.Series) -> tuple[float, float]:
    """Sum positive and negative altitude deltas on a subsampled series (meters)."""
    valid = altitudes.dropna().astype(int)
    if len(valid) < 2:
        return 0.0, 0.0

    sampled = valid.iloc[::ALTITUDE_SAMPLE_EVERY_N]
    deltas = sampled.diff().dropna()
    ascended = float(deltas[deltas > 0].sum())
    descended = float(-deltas[deltas < 0].sum())
    return ascended, descended
