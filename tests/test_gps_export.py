"""Tests for GPS export page builder and route."""

import pytest

from incognita.app import app
from incognita.data_models import TripDisplayStats
from incognita.gps_export import (
    build_gps_export_context,
    compute_paths_distance_km,
    format_calories,
    format_steps,
    validate_export_title,
)


def test_validate_export_title_rejects_empty():
    with pytest.raises(ValueError, match="Title is required"):
        validate_export_title("   ")


def test_format_steps():
    assert format_steps(None) == "—"
    assert format_steps(500) == "500"
    assert format_steps(15234) == "15.2k"


def test_format_calories():
    assert format_calories(None) == "—"
    assert format_calories(450.4) == "450 kcal"


def test_compute_paths_distance_km():
    paths = [[[0.0, 0.0], [0.0, 0.01]]]
    assert compute_paths_distance_km(paths) > 0


def test_format_display_date():
    from incognita.gps_export import format_display_date

    assert format_display_date("2026-07-15") == "15 July, 2026"
    assert format_display_date("2026-01-01") == "1 January, 2026"


def test_build_gps_export_context(monkeypatch):
    """Export context includes totals, averages, and all stat columns."""
    paths = [[[0.0, 0.0], [0.0, 0.01]]]
    trip_stats = TripDisplayStats(track_points=2, trips_count=1)
    health = {
        "totals": {"steps": 12000, "km": 8.5, "kcals": 900.0, "flights_climbed": 8},
        "days": [],
    }

    monkeypatch.setattr("incognita.gps_export.get_trips_for_date_range", lambda s, e: (paths, trip_stats))
    monkeypatch.setattr("incognita.gps_export.get_health_dump_for_date_range", lambda s, e: health)

    context = build_gps_export_context("My Trip", "2025-01-01", "2025-01-07")

    assert context["title"] == "My Trip"
    assert context["start_date_display"] == "1 January, 2025"
    assert context["end_date_display"] == "7 January, 2025"
    assert context["has_paths"] is True
    labels = [card["label"] for card in context["stat_cards"]]
    assert labels == ["Distance", "Walking", "Steps", "Calories", "Stairs"]
    assert context["stat_cards"][1]["value"] == "8.5 km"
    assert context["stat_cards"][1]["km"] == 8.5
    assert context["avg_stat_cards"][2]["value"] == "1.7k"
    assert context["avg_stat_cards"][3]["value"] == "129 kcal"


def test_gps_export_route_rejects_missing_title():
    with app.test_client() as client:
        response = client.get("/gps/export?start_date=2025-01-01&end_date=2025-01-01")

    assert response.status_code == 400


def test_gps_export_route_renders_title(monkeypatch):
    context = {
        "title": "Weekend ride",
        "start_date": "2025-01-01",
        "end_date": "2025-01-07",
        "start_date_display": "1 January, 2025",
        "end_date_display": "7 January, 2025",
        "day_count": 7,
        "paths": [],
        "bbox": None,
        "stat_cards": [],
        "avg_stat_cards": [],
        "has_paths": False,
    }
    monkeypatch.setattr("incognita.app.build_gps_export_context", lambda *args, **kwargs: context)

    with app.test_client() as client:
        response = client.get("/gps/export?title=Weekend+ride&start_date=2025-01-01&end_date=2025-01-07")

    assert response.status_code == 200
    assert b"Weekend ride" in response.data
