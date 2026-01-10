"""Tests for GPS data processing functions."""

import pytest

from incognita.processing import get_haversine_dist


@pytest.mark.parametrize(
    "lat1,lon1,lat2,lon2,expected_meters",
    [
        (0.0, 0.0, 0.0, 0.0, 0.0),
        (37.7749, -122.4194, 37.7749, -122.4194, 0.0),
        (37.7749, -122.4194, 37.7849, -122.4194, 1113.2),
        (0.0, 0.0, 0.0, 1.0, 111319.5),
        (-90.0, 0.0, 90.0, 0.0, 20037508.3),
    ],
)
def test_haversine_distance_calculation(
    lat1: float, lon1: float, lat2: float, lon2: float, expected_meters: float
):
    """Verify haversine distance matches expected values for known coordinates."""
    result = get_haversine_dist(lat1, lon1, lat2, lon2)

    assert abs(result - expected_meters) < 1.0


def test_haversine_distance_symmetry():
    """Verify distance from A to B equals distance from B to A."""
    lat1, lon1 = 37.7749, -122.4194
    lat2, lon2 = 40.7128, -74.0060

    distance_ab = get_haversine_dist(lat1, lon1, lat2, lon2)
    distance_ba = get_haversine_dist(lat2, lon2, lat1, lon1)

    assert abs(distance_ab - distance_ba) < 0.001
