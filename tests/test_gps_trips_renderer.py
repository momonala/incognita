from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from incognita.data_models import TripDisplayStats
from incognita.gps_trips_renderer import (
    _get_live_month_trips,
    get_trip_points_for_date_range,
    get_trips_for_date_range,
)


def test_get_trip_points_for_date_range_filters_to_requested_window(monkeypatch):
    """Return only simplified vertices that fall within the requested time window."""
    start = datetime(2024, 1, 10, 12, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 10, 13, 0, tzinfo=timezone.utc)

    monkeypatch.setattr("incognita.gps_trips_renderer._month_range", lambda *_: [(2024, 1)])
    monkeypatch.setattr(
        "incognita.gps_trips_renderer._get_live_month_trips",
        lambda **_: [
            [
                [1.0, 2.0, start.timestamp() - 1],
                [3.0, 4.0, start.timestamp() + 1],
                [5.0, 6.0, end.timestamp() - 1],
                [7.0, 8.0, end.timestamp() + 1],
            ],
            [[9.0, 10.0, start.timestamp() + 1]],
        ],
    )

    result = get_trip_points_for_date_range(start, end)

    assert result == [[[3.0, 4.0, start.timestamp() + 1], [5.0, 6.0, end.timestamp() - 1]]]


def test_get_live_month_trips_uses_day_cached_points(monkeypatch, tmp_path: Path):
    """Load current-month points from day caches before segmenting once."""
    day_one_root = tmp_path / "2024" / "02" / "09"
    day_two_root = tmp_path / "2024" / "02" / "10"
    day_one_root.mkdir(parents=True)
    day_two_root.mkdir(parents=True)

    monkeypatch.setattr("incognita.gps_trips_renderer._days_in_range_for_month", lambda *_: [9, 10])
    monkeypatch.setattr(
        "incognita.gps_trips_renderer._day_root",
        lambda year, month, day: day_one_root if day == 9 else day_two_root,
    )
    monkeypatch.setattr("incognita.gps_trips_renderer._compute_day_dir_hash", lambda root: root.name)

    def fake_get_day_points_cached_impl(year: int, month: int, day: int, day_hash: str) -> pd.DataFrame:
        if day == 9:
            return pd.DataFrame([{"timestamp": pd.Timestamp("2024-02-09T23:00:00Z"), "lon": 1.0, "lat": 2.0}])
        return pd.DataFrame([{"timestamp": pd.Timestamp("2024-02-10T01:00:00Z"), "lon": 3.0, "lat": 4.0}])

    def fake_gdf_to_simplified_trip_paths(
        gdf: pd.DataFrame,
        max_gap_seconds: float,
        max_gap_meters: float,
        min_points: int,
        simplify_tolerance_m: float,
        source_label: str,
    ) -> list[list[list[float]]]:
        assert list(gdf["timestamp"]) == [
            pd.Timestamp("2024-02-09T23:00:00Z"),
            pd.Timestamp("2024-02-10T01:00:00Z"),
        ]
        assert source_label == "_get_live_month_trips 2024-02"
        return [[[1.0, 2.0, 100.0], [3.0, 4.0, 200.0]]]

    monkeypatch.setattr(
        "incognita.gps_trips_renderer._get_day_points_cached_impl",
        fake_get_day_points_cached_impl,
    )
    monkeypatch.setattr(
        "incognita.gps_trips_renderer._gdf_to_simplified_trip_paths",
        fake_gdf_to_simplified_trip_paths,
    )

    result = _get_live_month_trips(
        year=2024,
        month=2,
        start=datetime(2024, 2, 9, tzinfo=timezone.utc),
        end=datetime(2024, 2, 10, tzinfo=timezone.utc),
        max_gap_seconds=60.0,
        max_gap_meters=100.0,
        min_points=10,
        simplify_tolerance_m=5.0,
    )

    assert result == [[[1.0, 2.0, 100.0], [3.0, 4.0, 200.0]]]


def test_get_trip_points_for_date_range_uses_month_cache_for_previous_months(monkeypatch, tmp_path: Path):
    """Keep month-level caching for months before the live month."""
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 2, 10, tzinfo=timezone.utc)
    january_root = tmp_path / "2024" / "01"
    january_root.mkdir(parents=True)
    month_calls: list[tuple[int, int]] = []

    monkeypatch.setattr("incognita.gps_trips_renderer._month_range", lambda *_: [(2024, 1), (2024, 2)])
    monkeypatch.setattr(
        "incognita.gps_trips_renderer._month_root",
        lambda year, month: january_root if month == 1 else tmp_path / "2024" / "02",
    )
    monkeypatch.setattr("incognita.gps_trips_renderer._compute_month_dir_hash", lambda *_: "monthhash")

    def fake_get_month_trips_cached_impl(**kwargs) -> list[list[list[float]]]:
        month_calls.append((kwargs["year"], kwargs["month"]))
        return [[[1.0, 2.0, start.timestamp() + 10], [3.0, 4.0, start.timestamp() + 20]]]

    monkeypatch.setattr(
        "incognita.gps_trips_renderer._get_month_trips_cached_impl",
        fake_get_month_trips_cached_impl,
    )
    monkeypatch.setattr(
        "incognita.gps_trips_renderer._get_live_month_trips",
        lambda **_: [[[5.0, 6.0, end.timestamp() - 20], [7.0, 8.0, end.timestamp() - 10]]],
    )

    result = get_trip_points_for_date_range(start, end)

    assert month_calls == [(2024, 1)]
    assert result == [
        [[1.0, 2.0, start.timestamp() + 10], [3.0, 4.0, start.timestamp() + 20]],
        [[5.0, 6.0, end.timestamp() - 20], [7.0, 8.0, end.timestamp() - 10]],
    ]


def test_get_trips_for_date_range_strips_timestamps(monkeypatch):
    """Return map paths without timestamps while preserving trip stats."""
    monkeypatch.setattr(
        "incognita.gps_trips_renderer.get_trip_points_for_date_range",
        lambda *_, **__: [[[1.0, 2.0, 100.0], [3.0, 4.0, 200.0]]],
    )

    paths, stats = get_trips_for_date_range(
        datetime(2024, 1, 10, tzinfo=timezone.utc),
        datetime(2024, 1, 11, tzinfo=timezone.utc),
    )

    assert paths == [[[1.0, 2.0], [3.0, 4.0]]]
    assert stats == TripDisplayStats(track_points=2, trips_count=1)


def test_get_trips_for_date_range_returns_empty_stats_when_no_points(monkeypatch):
    """Return empty stats when no simplified trip points are available."""
    monkeypatch.setattr("incognita.gps_trips_renderer.get_trip_points_for_date_range", lambda *_, **__: None)

    paths, stats = get_trips_for_date_range(
        datetime(2024, 1, 10, tzinfo=timezone.utc),
        datetime(2024, 1, 11, tzinfo=timezone.utc),
    )

    assert paths is None
    assert stats == TripDisplayStats(track_points=0, trips_count=0)
