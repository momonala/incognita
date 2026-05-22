"""Tests for HealthKit sample storage."""

import json
import sqlite3

import pytest
from pydantic import ValidationError

from incognita.data_api import app
from incognita.data_models import HealthKitBatch, HealthKitExportType, HealthKitSample
from incognita.health_database import insert_health_batch


@pytest.fixture
def health_db(tmp_path):
    db_path = tmp_path / "health_data.db"
    return str(db_path)


def _sample(**overrides) -> dict:
    base = {
        "type": "Step Count",
        "uuid": "AAA",
        "start": "2024-01-15T08:32:00Z",
        "end": "2024-01-15T08:32:00Z",
        "value": 500.0,
        "unit": "count",
        "source": "iPhone",
        "deviceName": "iPhone 16 Pro",
        "deviceModel": "iPhone16,2",
        "deviceManufacturer": "Apple Inc.",
        "deviceHardwareVersion": "iPhone16,2",
        "deviceSoftwareVersion": "18.0",
        "metadata": {},
    }
    base.update(overrides)
    return base


def test_healthkit_batch_validates_ios_payload():
    batch = HealthKitBatch.model_validate(
        {
            "batchIndex": 12,
            "samples": [_sample(), _sample(type="Flights Climbed", uuid="BBB", value=3.0)],
        }
    )

    assert batch.batch_index == 12
    assert len(batch.samples) == 2
    assert batch.samples[0].type == HealthKitExportType.STEP_COUNT
    assert batch.samples[0].device_name == "iPhone 16 Pro"
    assert batch.samples[0].device_model == "iPhone16,2"


def test_healthkit_sample_rejects_unknown_type():
    with pytest.raises(ValidationError):
        HealthKitSample.model_validate(_sample(type="Heart Rate"))


def test_insert_health_batch_routes_by_type(health_db):
    batch = HealthKitBatch.model_validate(
        {
            "batchIndex": 1,
            "samples": [
                _sample(),
                _sample(
                    type="Flights Climbed",
                    uuid="BBB",
                    value=3.0,
                    unit="count",
                    source="iPhone",
                    deviceName=None,
                    metadata={},
                ),
            ],
        }
    )

    inserted, skipped = insert_health_batch(batch, db_filename=health_db)

    assert inserted == 2
    assert skipped == 0

    with sqlite3.connect(health_db) as conn:
        step_rows = conn.execute("SELECT uuid, value, device_name FROM step_count").fetchall()
        flights_rows = conn.execute("SELECT uuid, value FROM flights_climbed").fetchall()

    assert step_rows == [("AAA", 500.0, "iPhone 16 Pro")]
    assert flights_rows == [("BBB", 3.0)]


def test_insert_health_batch_dedupes_by_uuid(health_db):
    sample = _sample(
        type="Distance",
        uuid="CCC",
        start="2024-01-15T10:00:00Z",
        end="2024-01-15T10:05:00Z",
        value=120.5,
        unit="m",
        source="iPhone",
        deviceName=None,
        metadata={},
    )
    first = HealthKitBatch.model_validate({"batchIndex": 1, "samples": [sample]})
    second = HealthKitBatch.model_validate({"batchIndex": 2, "samples": [sample]})

    insert_health_batch(first, db_filename=health_db)
    inserted, skipped = insert_health_batch(second, db_filename=health_db)

    assert inserted == 0
    assert skipped == 1

    with sqlite3.connect(health_db) as conn:
        row = conn.execute("SELECT batch_index FROM distance WHERE uuid = 'CCC'").fetchone()

    assert row == (1,)


def test_ios_dump_endpoint(monkeypatch):
    monkeypatch.setattr("incognita.data_api.insert_health_batch", lambda batch: (1, 0))

    payload = {
        "batchIndex": 5,
        "samples": [
            _sample(
                type="Active Energy",
                uuid="DDD",
                start="2024-01-15T12:00:00Z",
                end="2024-01-15T12:30:00Z",
                value=12.5,
                unit="kcal",
                deviceName=None,
                metadata={},
            )
        ],
    }

    with app.test_client() as client:
        response = client.post("/ios-dump", data=json.dumps(payload), content_type="application/json")

    assert response.status_code == 200
    assert response.get_json() == {"result": "ok", "inserted": 1, "skipped": 0}


def test_ios_dump_rejects_invalid_payload():
    payload = {"batchIndex": 1, "samples": [_sample(type="Unknown")]}

    with app.test_client() as client:
        response = client.post("/ios-dump", data=json.dumps(payload), content_type="application/json")

    assert response.status_code == 400
    assert response.get_json()["result"] == "error"


def test_insert_creates_all_tables(health_db):
    batch = HealthKitBatch.model_validate({"batchIndex": 0, "samples": []})
    insert_health_batch(batch, db_filename=health_db)

    with sqlite3.connect(health_db) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            ).fetchall()
        }

    assert tables == {export_type.table_name for export_type in HealthKitExportType}
