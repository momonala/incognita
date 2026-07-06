"""Tests for daily motion stats aggregation."""

import sqlite3
from datetime import datetime

import pytest

from incognita.database import DB_NAME
from incognita.motion_stats import (
    get_daily_motion_stats,
    get_motion_stats_range,
    invalidate_motion_stats_cache,
    _read_motion_stats_cache,
    _write_motion_stats_cache,
)
import incognita.motion_stats as motion_stats_module


@pytest.fixture
def motion_stats_db(tmp_path):
    """SQLite DB with a small day of walking, driving, and stationary points."""
    db_path = tmp_path / "geo_data.db"
    rows = [
        (-122.4194, 37.7749, "2025-01-01T10:00:00Z", 1.0, 100.0, "walking"),
        (-122.4194, 37.7760, "2025-01-01T10:01:00Z", 2.0, 110.0, "walking"),
        (-122.4194, 37.7760, "2025-01-01T10:02:00Z", 0.0, 110.0, "stationary"),
        (-122.42, 37.78, "2025-01-01T11:00:00Z", 15.0, 110.0, "driving"),
        (-122.41, 37.79, "2025-01-01T11:05:00Z", 20.0, 110.0, "automotive"),
        (-122.40, 37.80, "2025-01-01T12:00:00Z", 3.0, 110.0, None),
        (-122.39, 37.81, "2025-01-01T12:10:00Z", 4.0, 90.0, "running"),
    ]
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"""
            CREATE TABLE {DB_NAME} (
                lon REAL,
                lat REAL,
                timestamp TEXT,
                speed REAL,
                altitude REAL,
                horizontal_accuracy REAL,
                motion TEXT,
                geojson_file TEXT
            )
            """)
        conn.executemany(
            f"""
            INSERT INTO {DB_NAME}
            (lon, lat, timestamp, speed, altitude, horizontal_accuracy, motion, geojson_file)
            VALUES (?, ?, ?, ?, ?, 10.0, ?, 'test.geojson')
            """,
            rows,
        )
        conn.commit()
    return db_path


def test_get_daily_motion_stats_groups_motion_and_stationary_time(motion_stats_db):
    """Moving rows (speed > 0) aggregate by motion label; stationary time from motion=stationary."""
    stats = get_daily_motion_stats("2025-01-01", db_filename=str(motion_stats_db))

    assert stats["date"] == "2025-01-01"
    assert stats["total_km"] > 0
    assert stats["max_speed_m_s"] == 20.0
    assert stats["time_spent_seconds"] > 0
    assert stats["motion_type"]["stationary"]["distance_km"] == 0.0
    assert stats["motion_type"]["stationary"]["time_seconds"] == pytest.approx(60.0, rel=0.01)
    assert stats["motion_type"]["walking"]["distance_km"] > 0
    assert stats["motion_type"]["walking"]["time_seconds"] == pytest.approx(60.0, rel=0.01)
    assert stats["motion_type"]["automotive"]["distance_km"] > 0
    assert stats["motion_type"]["running"]["distance_km"] > 0
    assert stats["motion_type"]["cycling"]["distance_km"] == 0.0


def test_stationary_time_uses_speed_zero_rows(motion_stats_db):
    """Stationary duration includes rows with speed=0 that moving stats exclude."""
    stats = get_daily_motion_stats("2025-01-01", db_filename=str(motion_stats_db))

    assert stats["motion_type"]["stationary"]["time_seconds"] == pytest.approx(60.0, rel=0.01)
    assert stats["time_spent_seconds"] > stats["motion_type"]["stationary"]["time_seconds"]


def test_get_daily_motion_stats_raises_when_db_missing(tmp_path):
    """Missing database raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        get_daily_motion_stats("2025-01-01", db_filename=str(tmp_path / "missing.db"))


def test_motion_stats_cache_round_trip(motion_stats_db):
    stats = get_daily_motion_stats("2025-01-01", db_filename=str(motion_stats_db))
    _write_motion_stats_cache("2025-01-01", stats, str(motion_stats_db))
    assert _read_motion_stats_cache("2025-01-01", str(motion_stats_db)) == stats
    invalidate_motion_stats_cache(["2025-01-01"], str(motion_stats_db))
    assert _read_motion_stats_cache("2025-01-01", str(motion_stats_db)) is None


def test_motion_stats_range_uses_cache_for_past_days(motion_stats_db, monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2025, 1, 2, 12, 0, 0)

    monkeypatch.setattr(motion_stats_module, "datetime", FixedDatetime)

    cached = get_daily_motion_stats("2025-01-01", db_filename=str(motion_stats_db))
    cached["total_km"] = 42.0
    _write_motion_stats_cache("2025-01-01", cached, str(motion_stats_db))

    rows = get_motion_stats_range(2, db_filename=str(motion_stats_db))
    assert rows[0]["total_km"] == 42.0
    assert rows[1]["date"] == "2025-01-02"
    assert _read_motion_stats_cache("2025-01-02", str(motion_stats_db)) is None
