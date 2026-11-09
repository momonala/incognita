"""Tests for database module functions."""

import gzip
import json
import sqlite3
from unittest.mock import patch

import pytest

from incognita.database import extract_properties_from_geojson
from incognita.database import filter_by_accuracy
from incognita.database import find_geojson_files
from incognita.database import pack_geojson
from incognita.database import read_geojson_file
from incognita.database import update_db
from incognita.database import write_geojson_file


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
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump({"locations": locations}, f)
    return str(path)


def _location(timestamp="2024-01-01T12:00:00Z"):
    return {
        "geometry": {"coordinates": [-122.4194, 37.7749]},
        "properties": {"timestamp": timestamp, "horizontal_accuracy": 10.0},
    }


def test_read_geojson_file_reads_gzipped_locations(tmp_path):
    path = _write_geojson(tmp_path / "packed.geojson.gz", [_location("2024-01-02T12:00:00Z")])

    rows = read_geojson_file(path)

    assert rows[0]["properties"]["timestamp"] == "2024-01-02T12:00:00Z"
    assert rows[0]["geojson_file"] == path


def test_read_geojson_file_returns_none_for_truncated_gzip(tmp_path):
    gz_path = tmp_path / "cut.geojson.gz"
    gz_path.write_bytes(gzip.compress(json.dumps({"locations": [_location()]}).encode())[:-10])

    assert read_geojson_file(str(gz_path)) is None


def test_find_geojson_files_finds_gzipped_only(tmp_path):
    (tmp_path / "2024" / "01").mkdir(parents=True)
    (tmp_path / "2024" / "01" / "a.geojson.gz").write_bytes(b"")
    (tmp_path / "2024" / "01" / "b.geojson").write_text("{}")

    assert [p.name for p in find_geojson_files(tmp_path)] == ["a.geojson.gz"]


def test_write_geojson_file_round_trips_through_read_and_is_compact(tmp_path):
    path = tmp_path / "out.geojson.gz"
    payload = {"locations": [_location()]}

    write_geojson_file(path, pack_geojson(payload))

    assert read_geojson_file(str(path))[0]["properties"] == _location()["properties"]
    raw = gzip.decompress(path.read_bytes()).decode()
    assert "\n" not in raw
    assert ": " not in raw
    assert list(tmp_path.iterdir()) == [path]


def test_write_geojson_file_leaves_nothing_behind_when_the_rename_fails(tmp_path, monkeypatch):
    path = tmp_path / "out.geojson.gz"

    def fail(*_args):
        raise OSError("disk gone")

    monkeypatch.setattr("incognita.database.os.replace", fail)

    with pytest.raises(OSError):
        write_geojson_file(path, pack_geojson({"locations": []}))

    assert list(tmp_path.iterdir()) == []


@patch("incognita.database.metrics")
def test_update_db_reports_success(mock_metrics, tmp_path):
    geojson_file = _write_geojson(tmp_path / "valid.geojson.gz", [_location()])
    db_file = str(tmp_path / "geo_data.db")

    update_db(geojson_file, db_filename=db_file)

    mock_metrics.increment.assert_called_once_with("success")
    with sqlite3.connect(db_file) as conn:
        rows = conn.execute("SELECT timestamp FROM overland").fetchall()
    assert rows == [("2024-01-01T12:00:00Z",)]


@patch("incognita.database.metrics")
def test_update_db_reports_parse_failure(mock_metrics, tmp_path):
    geojson_file = tmp_path / "malformed.geojson.gz"
    geojson_file.write_bytes(gzip.compress(b"not json"))

    update_db(str(geojson_file), db_filename=str(tmp_path / "geo_data.db"))

    mock_metrics.increment.assert_called_once_with("error", tags={"kind": "parse_failure"})


@patch("incognita.database.metrics")
def test_update_db_reports_empty(mock_metrics, tmp_path):
    # horizontal_accuracy above the default 200m threshold is filtered out entirely,
    # leaving a non-empty raw_geojson but an empty parsed DataFrame.
    inaccurate_location = _location()
    inaccurate_location["properties"]["horizontal_accuracy"] = 500.0
    geojson_file = _write_geojson(tmp_path / "inaccurate.geojson.gz", [inaccurate_location])

    update_db(geojson_file, db_filename=str(tmp_path / "geo_data.db"))

    mock_metrics.increment.assert_called_once_with("error", tags={"kind": "empty"})
