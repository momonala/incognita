"""Tests for database module functions."""

from incognita.database import extract_properties_from_geojson, filter_by_accuracy


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
