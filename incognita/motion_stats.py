"""Daily GPS motion statistics from the Overland SQLite database."""

import sqlite3
from pathlib import Path

import pandas as pd

from incognita.database import DB_FILE, DB_NAME
from incognita.gps_point_series import add_speed_to_gdf

MOTION_CATEGORIES = ("automotive", "cycling", "running", "stationary", "unknown", "walking")

ALTITUDE_SAMPLE_EVERY_N = 60

_DAILY_POINTS_SQL = f"""
SELECT lon, lat, timestamp, speed, altitude, motion
FROM {DB_NAME}
WHERE timestamp >= ? AND timestamp <= ?
ORDER BY timestamp ASC
"""


def get_daily_motion_stats(date: str, db_filename: str = DB_FILE) -> dict:
    """Return daily motion stats for a calendar day (YYYY-MM-DD)."""
    db_path = Path(db_filename)
    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {db_filename}")

    day_points = _load_daily_points(date, db_filename)
    return _aggregate_motion_stats(day_points, date)


def _load_daily_points(date: str, db_filename: str) -> pd.DataFrame:
    date_max = f"{date}T23:59:59Z"
    with sqlite3.connect(db_filename) as conn:
        return pd.read_sql(_DAILY_POINTS_SQL, conn, params=[date, date_max])


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
