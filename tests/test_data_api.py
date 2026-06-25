"""Tests for data API utility functions."""

import pytest

from incognita.data_api import app, format_downtime


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
