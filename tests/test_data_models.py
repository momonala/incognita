"""Tests for Pydantic data models."""

import pytest
from pydantic import ValidationError

from incognita.data_models import GeoBoundingBox, GeoCoords, HealthKitBatch


def test_geo_bounding_box_derives_corners_from_center():
    bbox = GeoBoundingBox(center=GeoCoords(lat=52.52, lon=13.405), width=0.1, name="Berlin")

    assert bbox.name == "Berlin"
    assert bbox.center.lat == pytest.approx(52.52)
    assert bbox.sw.lat == pytest.approx(52.42)
    assert bbox.sw.lon == pytest.approx(13.305)
    assert bbox.ne.lat == pytest.approx(52.62)
    assert bbox.ne.lon == pytest.approx(13.505)


def test_geo_bounding_box_accepts_explicit_corners():
    center = GeoCoords(lat=1.0, lon=2.0)
    sw = GeoCoords(lat=0.0, lon=1.0)
    ne = GeoCoords(lat=2.0, lon=3.0)

    bbox = GeoBoundingBox(center=center, sw=sw, ne=ne, name="custom")

    assert bbox.sw == sw
    assert bbox.ne == ne


def test_healthkit_batch_validates_batch_index_alias():
    batch = HealthKitBatch.model_validate({"batchIndex": 3, "samples": []})

    assert batch.batch_index == 3


def test_geo_coords_rejects_non_numeric():
    with pytest.raises(ValidationError):
        GeoCoords.model_validate({"lat": "north", "lon": 13.0})
