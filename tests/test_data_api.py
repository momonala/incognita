"""Tests for data API utility functions."""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from incognita import data_api
from incognita.config import SPYGLASS_DASHBOARD_URL
from incognita.data_api import app, format_downtime
from incognita.utils import BYTES_PER_MB


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (45, "0m, 45s"),
        (90, "1m, 30s"),
        (3600, "1h, 0m, 0s"),
        (3661, "1h, 1m, 1s"),
        (86400, "1d, 0h, 0m, 0s"),
        (90061, "1d, 1h, 1m, 1s"),
        (0, "0m, 0s"),
    ],
)
def test_format_downtime(seconds: int, expected: str):
    """Verify downtime is formatted correctly for various durations."""
    result = format_downtime(seconds)

    assert result == expected


def test_format_downtime_truncates_subseconds():
    """Verify subsecond precision is truncated."""
    result = format_downtime(90.999)

    assert result == "1m, 30s"


def test_coordinates_uses_file_pipeline(monkeypatch):
    """Verify /coordinates uses the file-backed simplified trip path."""
    calls: dict[str, object] = {}

    def fake_get_trip_points_for_date_range(start_dt, end_dt):
        calls["start_dt"] = start_dt
        calls["end_dt"] = end_dt
        return [[[13.405, 52.52, 1735732800.0], [13.41, 52.53, 1735736400.0]]]

    monkeypatch.setattr(
        "incognita.data_api.get_trip_points_for_date_range", fake_get_trip_points_for_date_range
    )

    with app.test_client() as client:
        response = client.get("/coordinates?lookback_hours=24")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload == {
        "status": "success",
        "count": 2,
        "lookback_hours": 24,
        "paths": [
            [
                {
                    "timestamp": "2025-01-01T12:00:00Z",
                    "latitude": 52.52,
                    "longitude": 13.405,
                },
                {
                    "timestamp": "2025-01-01T13:00:00Z",
                    "latitude": 52.53,
                    "longitude": 13.41,
                },
            ]
        ],
    }
    assert round((calls["end_dt"] - calls["start_dt"]).total_seconds()) == 24 * 60 * 60


def test_coordinates_preserves_trip_segments(monkeypatch):
    """Return separate paths so clients do not connect across missing-data gaps."""

    monkeypatch.setattr(
        "incognita.data_api.get_trip_points_for_date_range",
        lambda *_: [
            [[13.405, 52.52, 1735732800.0], [13.41, 52.53, 1735736400.0]],
            [[13.5, 52.6, 1735740000.0], [13.51, 52.61, 1735743600.0]],
        ],
    )

    with app.test_client() as client:
        response = client.get("/coordinates?lookback_hours=24")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["count"] == 4
    assert payload["paths"] == [
        [
            {
                "timestamp": "2025-01-01T12:00:00Z",
                "latitude": 52.52,
                "longitude": 13.405,
            },
            {
                "timestamp": "2025-01-01T13:00:00Z",
                "latitude": 52.53,
                "longitude": 13.41,
            },
        ],
        [
            {
                "timestamp": "2025-01-01T14:00:00Z",
                "latitude": 52.6,
                "longitude": 13.5,
            },
            {
                "timestamp": "2025-01-01T15:00:00Z",
                "latitude": 52.61,
                "longitude": 13.51,
            },
        ],
    ]


def test_coordinates_by_date_queries_that_local_day(monkeypatch):
    """A date query covers exactly one local calendar day and echoes the date back."""
    calls: dict[str, datetime] = {}

    def fake_get_trip_points_for_date_range(start_dt, end_dt):
        calls["start_dt"] = start_dt
        calls["end_dt"] = end_dt
        return [[[13.405, 52.52, 1735732800.0], [13.41, 52.53, 1735736400.0]]]

    monkeypatch.setattr(
        "incognita.data_api.get_trip_points_for_date_range", fake_get_trip_points_for_date_range
    )

    with app.test_client() as client:
        response = client.get("/coordinates?date=2025-01-01")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["date"] == "2025-01-01"
    assert payload["count"] == 2
    assert "lookback_hours" not in payload

    # Window is UTC (raw dirs are UTC-partitioned) but spans one local midnight-to-midnight day.
    start_local = calls["start_dt"].astimezone()
    end_local = calls["end_dt"].astimezone()
    assert (start_local.year, start_local.month, start_local.day) == (2025, 1, 1)
    assert (start_local.hour, start_local.minute) == (0, 0)
    assert (end_local.year, end_local.month, end_local.day) == (2025, 1, 2)
    assert (end_local.hour, end_local.minute) == (0, 0)


def test_coordinates_today_clamps_window_to_now(monkeypatch):
    """date=today must not ask for the remainder of the day, which has no data yet."""
    calls: dict[str, datetime] = {}

    def fake_get_trip_points_for_date_range(start_dt, end_dt):
        calls["end_dt"] = end_dt
        return None

    monkeypatch.setattr(
        "incognita.data_api.get_trip_points_for_date_range", fake_get_trip_points_for_date_range
    )

    with app.test_client() as client:
        response = client.get("/coordinates?date=today")

    assert response.status_code == 200
    assert response.get_json()["date"] == datetime.now().strftime("%Y-%m-%d")
    assert abs((datetime.now(timezone.utc) - calls["end_dt"]).total_seconds()) < 60


def test_coordinates_by_date_with_no_data_is_empty_not_error(monkeypatch):
    """An untracked day is a successful empty response, not a failure."""
    monkeypatch.setattr("incognita.data_api.get_trip_points_for_date_range", lambda *_: None)

    with app.test_client() as client:
        response = client.get("/coordinates?date=2019-06-15")

    assert response.status_code == 200
    assert response.get_json() == {"status": "success", "count": 0, "date": "2019-06-15", "paths": []}


def test_coordinates_future_date_returns_empty(monkeypatch):
    """A day that has not started yet yields an empty window without hitting the loader."""
    monkeypatch.setattr(
        "incognita.data_api.get_trip_points_for_date_range",
        lambda *_: pytest.fail("loader should not run for a future date"),
    )
    future = (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%d")

    with app.test_client() as client:
        response = client.get(f"/coordinates?date={future}")

    assert response.status_code == 200
    assert response.get_json()["count"] == 0


@pytest.mark.parametrize("query", ["date=not-a-date", "date=2025-13-01", "date=2025-01-01&lookback_hours=24"])
def test_coordinates_rejects_bad_date_queries(query: str):
    """Malformed dates and date+lookback_hours together are client errors."""
    with app.test_client() as client:
        response = client.get(f"/coordinates?{query}")

    assert response.status_code == 400
    assert response.get_json()["status"] == "error"


def test_motion_stats_returns_daily_summary(monkeypatch):
    """Verify /motion-stats returns the DailyMotionStats payload."""
    sample = {
        "date": "2025-01-01",
        "total_km": 12.5,
        "max_speed_m_s": 20.0,
        "avg_speed_m_s": 5.0,
        "time_spent_seconds": 3600.0,
        "altitude_ascended_m": 150.0,
        "altitude_descended_m": 75.0,
        "motion_type": {
            "automotive": {"distance_km": 6.5, "time_seconds": 3000.0},
            "cycling": {"distance_km": 3.0, "time_seconds": 300.0},
            "running": {"distance_km": 1.5, "time_seconds": 150.0},
            "stationary": {"distance_km": 0.0, "time_seconds": 900.0},
            "unknown": {"distance_km": 1.0, "time_seconds": 100.0},
            "walking": {"distance_km": 2.0, "time_seconds": 200.0},
        },
    }
    monkeypatch.setattr("incognita.data_api.get_daily_motion_stats", lambda date: sample)

    with app.test_client() as client:
        response = client.get("/motion-stats?date=2025-01-01")

    assert response.status_code == 200
    assert response.get_json() == sample


def test_motion_stats_range_returns_ordered_days(monkeypatch):
    """Verify /motion-stats-range returns N daily stats oldest to newest."""
    sample = {
        "date": "2025-01-01",
        "total_km": 1.0,
        "max_speed_m_s": 1.0,
        "avg_speed_m_s": 1.0,
        "time_spent_seconds": 60.0,
        "altitude_ascended_m": 0.0,
        "altitude_descended_m": 0.0,
        "motion_type": {
            "automotive": {"distance_km": 0.0, "time_seconds": 0.0},
            "cycling": {"distance_km": 0.0, "time_seconds": 0.0},
            "running": {"distance_km": 0.0, "time_seconds": 0.0},
            "stationary": {"distance_km": 0.0, "time_seconds": 0.0},
            "unknown": {"distance_km": 0.0, "time_seconds": 0.0},
            "walking": {"distance_km": 1.0, "time_seconds": 60.0},
        },
    }

    def fake_range(days: int):
        assert days == 3
        return [
            {**sample, "date": "2025-01-01"},
            {**sample, "date": "2025-01-02", "total_km": 2.0},
            {**sample, "date": "2025-01-03", "total_km": 3.0},
        ]

    monkeypatch.setattr("incognita.data_api.get_motion_stats_range", fake_range)

    with app.test_client() as client:
        response = client.get("/motion-stats-range?days=3")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["days"] == 3
    assert [row["date"] for row in payload["stats"]] == ["2025-01-01", "2025-01-02", "2025-01-03"]
    assert payload["stats"][-1]["total_km"] == 3.0


@pytest.mark.parametrize("days", ["0", "367", "abc", "-1"])
def test_motion_stats_range_rejects_invalid_days(days):
    """/motion-stats-range only accepts integer days in 1–366."""
    with app.test_client() as client:
        response = client.get(f"/motion-stats-range?days={days}")

    assert response.status_code == 400


def test_health_data_range_returns_ordered_days(monkeypatch):
    """Verify /health-data-range returns N daily health rows oldest to newest."""
    monkeypatch.setattr(
        "incognita.data_api.get_health_dump_range",
        lambda days: [
            {"date": "2025-01-01", "steps": 1000, "kcals": 10.0, "km": 1.0, "flights_climbed": 2},
            {"date": "2025-01-02", "steps": 2000, "kcals": 20.0, "km": 2.0, "flights_climbed": 4},
        ],
    )

    with app.test_client() as client:
        response = client.get("/health-data-range?days=2")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["days"] == 2
    assert [row["date"] for row in payload["health"]] == ["2025-01-01", "2025-01-02"]
    assert payload["health"][1]["steps"] == 2000


def test_snooze_sets_window_and_mutes_alerts(monkeypatch):
    """A valid /snooze call records a future window and suppresses alerts within it."""
    import incognita.data_api as data_api

    monkeypatch.setattr(data_api, "snooze_until", None)
    monkeypatch.setattr(data_api, "_post_telegram", lambda text: 1)

    with app.test_client() as client:
        response = client.post("/snooze", json={"hours": 3})

    assert response.status_code == 200
    assert response.get_json()["status"] == "ok"
    assert data_api.snooze_until is not None
    assert data_api.alerts_muted() == f"snoozed until {data_api.snooze_until:%H:%M}"


@pytest.mark.parametrize("hours", [0, 25, -1, "abc", None])
def test_snooze_rejects_out_of_range(hours):
    """/snooze only accepts integer hours in 1–24."""
    with app.test_client() as client:
        response = client.post("/snooze", json={"hours": hours})

    assert response.status_code == 400


def test_alerts_muted_during_quiet_hours():
    """Overnight quiet hours mute alerts even without a snooze."""
    from datetime import datetime

    import incognita.data_api as data_api

    assert data_api.alerts_muted(datetime(2025, 1, 1, 2, 0)) == "sleepy time"
    assert data_api.alerts_muted(datetime(2025, 1, 1, 12, 0)) is None


def _dump_payload():
    return {
        "locations": [
            {
                "geometry": {"coordinates": [-122.4194, 37.7749]},
                "properties": {"timestamp": "2024-01-01T12:00:00Z", "horizontal_accuracy": 10.0},
            }
        ]
    }


@patch("incognita.data_api.metrics")
def test_dump_reports_received_and_written(mock_metrics, monkeypatch, tmp_path):
    """A new file received via /dump reports files_received, locations_count, and wrote_file."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("incognita.data_api.update_db", lambda filename: None)

    with app.test_client() as client:
        response = client.post("/dump", data=json.dumps(_dump_payload()), content_type="application/json")

    assert response.status_code == 200
    mock_metrics.increment.assert_any_call("files_received")
    mock_metrics.gauge.assert_any_call("locations_count", 1)
    mock_metrics.increment.assert_any_call("wrote_file")


@patch("incognita.data_api.metrics")
def test_dump_reports_duplicate_skipped(mock_metrics, monkeypatch, tmp_path):
    """Posting the same content twice reports duplicate_skipped on the second dump."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("incognita.data_api.update_db", lambda filename: None)
    payload = json.dumps(_dump_payload())

    with app.test_client() as client:
        client.post("/dump", data=payload, content_type="application/json")
        mock_metrics.reset_mock()
        client.post("/dump", data=payload, content_type="application/json")

    mock_metrics.increment.assert_any_call("duplicate_skipped")


def test_observability_redirects_to_spyglass_dashboard():
    with app.test_client() as client:
        response = client.get("/observability", follow_redirects=False)

    assert response.status_code == 302
    assert response.headers["Location"] == SPYGLASS_DASHBOARD_URL


def test_count_files_counts_nested_files_only(tmp_path):
    """Verify _count_files recurses through subdirectories and counts only files."""
    (tmp_path / "2025" / "01" / "01" / "12").mkdir(parents=True)
    (tmp_path / "2025" / "01" / "01" / "12" / "a.geojson").write_text("{}")
    (tmp_path / "2025" / "01" / "01" / "12" / "b.geojson").write_text("{}")
    (tmp_path / "2025" / "01" / "02").mkdir(parents=True)
    (tmp_path / "2025" / "01" / "02" / "c.geojson").write_text("{}")

    assert data_api._count_files(tmp_path) == 3


@patch("incognita.data_api.metrics")
def test_report_storage_metrics_reports_file_count_and_db_size(mock_metrics, monkeypatch, tmp_path):
    """Verify report_storage_metrics gauges raw-data file count and combined DB size in MB."""
    raw_data_root = tmp_path / "incognita_raw_data" / "2025" / "01" / "01" / "12"
    raw_data_root.mkdir(parents=True)
    (raw_data_root / "a.geojson").write_text("{}")
    (raw_data_root / "b.geojson").write_text("{}")
    monkeypatch.setattr(data_api, "RAW_DATA_ROOT", tmp_path / "incognita_raw_data")

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    geo_db = data_dir / "geo_data.db"
    geo_db.write_bytes(b"x" * BYTES_PER_MB)
    health_db = data_dir / "health_data.db"
    health_db.write_bytes(b"x" * BYTES_PER_MB)
    (data_dir / "health_data.db-wal").write_bytes(b"x" * (BYTES_PER_MB // 2))
    monkeypatch.setattr(data_api, "DB_FILE", str(geo_db))
    monkeypatch.setattr(data_api, "HEALTH_DB_FILE", str(health_db))

    data_api.report_storage_metrics()

    mock_metrics.gauge.assert_any_call("raw_data_file_count", 2)
    mock_metrics.gauge.assert_any_call("db_size_mb", pytest.approx(2.5))


@patch("incognita.data_api.metrics")
def test_report_process_metrics_reports_rss_and_gc_objects(mock_metrics):
    """Verify report_process_metrics gauges a positive RSS and GC-tracked object count."""
    data_api.report_process_metrics()

    gauged = {call.args[0]: call.args[1] for call in mock_metrics.gauge.call_args_list}
    assert gauged.keys() == {"rss_mb", "gc_objects"}
    assert gauged["rss_mb"] > 0
    assert gauged["gc_objects"] > 0
