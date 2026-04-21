"""Tests for data API utility functions."""

import pytest

from incognita.data_api import EARLIEST_HISTORY_DT, app, format_downtime


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


def test_coordinates_bbox_filters_by_first_point(monkeypatch):
    """Region-mode bbox keeps only trips whose first point is inside the bbox.

    The fake renderer returns three trips. Only the first one starts inside the
    Berlin-ish bbox; the second starts in Paris and the third is empty.
    """
    captured: dict[str, object] = {}

    def fake_get_trip_points_for_date_range(start_dt, end_dt):
        captured["start_dt"] = start_dt
        captured["end_dt"] = end_dt
        return [
            [[13.405, 52.52, 1735732800.0], [13.41, 52.53, 1735736400.0]],
            [[2.35, 48.85, 1735740000.0], [2.36, 48.86, 1735743600.0]],
            [],
        ]

    monkeypatch.setattr(
        "incognita.data_api.get_trip_points_for_date_range", fake_get_trip_points_for_date_range
    )

    with app.test_client() as client:
        response = client.get(
            "/coordinates"
            "?min_lat=52.4&max_lat=52.6&min_lon=13.0&max_lon=13.7"
            "&lookback_hours=24"  # should be ignored in region mode
        )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "success"
    assert payload["lookback_hours"] is None
    assert payload["bbox"] == {
        "min_lat": 52.4,
        "max_lat": 52.6,
        "min_lon": 13.0,
        "max_lon": 13.7,
    }
    assert payload["count"] == 2
    assert len(payload["paths"]) == 1
    assert payload["paths"][0][0] == {
        "timestamp": "2025-01-01T12:00:00Z",
        "latitude": 52.52,
        "longitude": 13.405,
    }
    assert captured["start_dt"] == EARLIEST_HISTORY_DT


@pytest.mark.parametrize(
    "query",
    [
        "?min_lat=52.4",
        "?min_lat=52.4&max_lat=52.6",
        "?min_lat=52.4&max_lat=52.6&min_lon=13.0",
        "?max_lat=52.6&min_lon=13.0&max_lon=13.7",
    ],
)
def test_coordinates_partial_bbox_returns_400(monkeypatch, query):
    """Partial bbox must fail with HTTP 400 and never hit the renderer."""

    def fail_if_called(*args, **kwargs):
        raise AssertionError("renderer should not be called for invalid bbox")

    monkeypatch.setattr("incognita.data_api.get_trip_points_for_date_range", fail_if_called)

    with app.test_client() as client:
        response = client.get(f"/coordinates{query}")

    assert response.status_code == 400
    assert response.get_json()["status"] == "error"
