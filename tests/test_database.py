"""Tests for database module functions."""

import json
import sqlite3
from unittest.mock import patch

from incognita.database import extract_properties_from_geojson, filter_by_accuracy, update_db


def test_filter_by_accuracy_removes_inaccurate_points():
    """Verify points with accuracy worse than threshold are filtered out."""
    geo_data = [
        {"properties": {"horizontal_accuracy": 50.0}},
        {"properties": {"horizontal_accuracy": 150.0}},
        {"properties": {"horizontal_accuracy": 250.0}},
    ]

    result = filter_by_accuracy(geo_data, min_horizontal_accuracy=200.0)

    assert len(result) == 2
    assert all(point["properties"]["horizontal_accuracy"] <= 200.0 for point in result)


def test_filter_by_accuracy_handles_missing_accuracy():
    """Verify points missing horizontal_accuracy field are filtered out."""
    geo_data = [
        {"properties": {"horizontal_accuracy": 50.0}},
        {"properties": {}},
        {"properties": {"other_field": "value"}},
    ]

    result = filter_by_accuracy(geo_data, min_horizontal_accuracy=200.0)

    assert len(result) == 1


def test_extract_properties_parses_valid_geojson():
    """Verify GeoJSON structure is correctly parsed into flat dictionary."""
    geo_data = [
        {
            "geometry": {"coordinates": [-122.4194, 37.7749]},
            "properties": {
                "timestamp": "2024-01-01T12:00:00Z",
                "horizontal_accuracy": 10.0,
                "speed": 1.5,
                "altitude": 50.0,
                "motion": ["walking"],
            },
            "geojson_file": "test.geojson",
        }
    ]

    result = extract_properties_from_geojson(geo_data, min_horizontal_accuracy=200.0)

    assert len(result) == 1
    assert result[0]["lon"] == -122.4194
    assert result[0]["lat"] == 37.7749
    assert result[0]["timestamp"] == "2024-01-01T12:00:00Z"
    assert result[0]["motion"] == "walking"
    assert result[0]["geojson_file"] == "test.geojson"


def test_extract_properties_handles_empty_motion():
    """Verify empty motion array is converted to None."""
    geo_data = [
        {
            "geometry": {"coordinates": [-122.4194, 37.7749]},
            "properties": {
                "timestamp": "2024-01-01T12:00:00Z",
                "horizontal_accuracy": 10.0,
                "motion": [],
            },
            "geojson_file": "test.geojson",
        }
    ]

    result = extract_properties_from_geojson(geo_data, min_horizontal_accuracy=200.0)

    assert result[0]["motion"] is None


def test_extract_properties_skips_entries_missing_required_fields():
    """Verify entries missing required coordinate/timestamp fields are skipped."""
    geo_data = [
        {
            "geometry": {"coordinates": [-122.4194, 37.7749]},
            "properties": {
                "timestamp": "2024-01-01T12:00:00Z",
                "horizontal_accuracy": 10.0,
            },
            "geojson_file": "test.geojson",
        },
        {
            "geometry": {"coordinates": [-122.5, 37.8]},
            "properties": {
                "horizontal_accuracy": 15.0,
            },
            "geojson_file": "test.geojson",
        },
        {
            "geometry": {"coordinates": [-122.6, 37.9]},
            "properties": {
                "timestamp": "2024-01-01T14:00:00Z",
                "horizontal_accuracy": 20.0,
            },
            "geojson_file": "test.geojson",
        },
    ]

    result = extract_properties_from_geojson(geo_data, min_horizontal_accuracy=200.0)

    assert len(result) == 2


def _write_geojson(path, locations):
    path.write_text(json.dumps({"locations": locations}))
    return str(path)


def _location(timestamp="2024-01-01T12:00:00Z"):
    return {
        "geometry": {"coordinates": [-122.4194, 37.7749]},
        "properties": {"timestamp": timestamp, "horizontal_accuracy": 10.0},
    }


@patch("incognita.database.metrics")
def test_update_db_reports_success(mock_metrics, tmp_path):
    geojson_file = _write_geojson(tmp_path / "valid.geojson", [_location()])
    db_file = str(tmp_path / "geo_data.db")

    update_db(geojson_file, db_filename=db_file)

    mock_metrics.increment.assert_called_once_with("success")
    with sqlite3.connect(db_file) as conn:
        rows = conn.execute("SELECT timestamp FROM overland").fetchall()
    assert rows == [("2024-01-01T12:00:00Z",)]


@patch("incognita.database.metrics")
def test_update_db_reports_parse_failure(mock_metrics, tmp_path):
    geojson_file = tmp_path / "malformed.geojson"
    geojson_file.write_text("not json")

    update_db(str(geojson_file), db_filename=str(tmp_path / "geo_data.db"))

    mock_metrics.increment.assert_called_once_with("error", tags={"kind": "parse_failure"})


@patch("incognita.database.metrics")
def test_update_db_reports_empty(mock_metrics, tmp_path):
    # horizontal_accuracy above the default 200m threshold is filtered out entirely,
    # leaving a non-empty raw_geojson but an empty parsed DataFrame.
    inaccurate_location = _location()
    inaccurate_location["properties"]["horizontal_accuracy"] = 500.0
    geojson_file = _write_geojson(tmp_path / "inaccurate.geojson", [inaccurate_location])

    update_db(geojson_file, db_filename=str(tmp_path / "geo_data.db"))

    mock_metrics.increment.assert_called_once_with("error", tags={"kind": "empty"})
