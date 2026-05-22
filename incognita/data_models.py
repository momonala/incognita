import json
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class HealthDump(BaseModel):
    """Daily health summary returned by the /health-data endpoint."""

    date: str  # YYYY-MM-DD
    steps: int | None = None
    kcals: float | None = None
    km: float | None = None
    flights_climbed: int | None = None
    weight: float | None = None  # kg — optional, not stored in HealthKit tables
    recorded_at: datetime


_FROZEN = ConfigDict(frozen=True, populate_by_name=True)


class GeoCoords(BaseModel):
    """Latitude and longitude (WGS84)."""

    model_config = _FROZEN

    lat: float
    lon: float


class GeoBoundingBox(BaseModel):
    """Map viewport from a center point and half-width in degrees."""

    model_config = _FROZEN

    center: GeoCoords
    sw: GeoCoords
    ne: GeoCoords
    name: str = ""

    @model_validator(mode="before")
    @classmethod
    def derive_corners(cls, data: object) -> object:
        # Pydantic may pass non-dict input (e.g. re-validating an existing model instance);
        # return it unchanged so Pydantic can handle it normally.
        if not isinstance(data, dict):
            return data
        if "sw" in data and "ne" in data:
            return {key: value for key, value in data.items() if key != "width"}

        center_raw = data.get("center")
        if center_raw is None:
            raise ValueError("center is required")

        center = center_raw if isinstance(center_raw, GeoCoords) else GeoCoords.model_validate(center_raw)
        width = data.get("width", 0.05)
        name = data.get("name", "")

        return {
            "center": center,
            "sw": GeoCoords(lat=center.lat - width, lon=center.lon - width),
            "ne": GeoCoords(lat=center.lat + width, lon=center.lon + width),
            "name": name,
        }


class Country(BaseModel):
    """Country record compatible with the pycountry Country interface, for non-ISO entries."""

    model_config = _FROZEN

    alpha_2: str
    alpha_3: str
    name: str
    flag: str


class LiveLocationSnapshot(BaseModel):
    """Most recent GPS fix and the full day's simplified trip paths."""

    model_config = _FROZEN

    lat: float
    lon: float
    timestamp: datetime  # timezone-aware UTC
    day_paths: list[list[list[float]]]  # per trip: [[lon, lat, unix_ts_sec], ...]


class TripDisplayStats(BaseModel):
    """Display stats for the GPS trips map (no raw GPS point count)."""

    model_config = _FROZEN

    track_points: int  # Vertices in simplified trip paths
    trips_count: int


class HealthKitExportType(StrEnum):
    """Display names sent by the iOS HealthKit export app."""

    STEP_COUNT = "Step Count"
    DISTANCE = "Distance"
    ACTIVE_ENERGY = "Active Energy"
    FLIGHTS_CLIMBED = "Flights Climbed"

    @property
    def table_name(self) -> str:
        """SQLite table name derived from the enum member name (e.g. HEART_RATE → heart_rate)."""
        return self.name.lower()


class HealthKitSample(BaseModel):
    """One HealthKit quantity sample from an iOS export batch."""

    model_config = _FROZEN

    type: HealthKitExportType
    uuid: str
    start: str
    end: str
    value: float | None = None
    unit: str | None = None
    source: str
    device_name: str | None = Field(None, alias="deviceName")
    device_model: str | None = Field(None, alias="deviceModel")
    device_manufacturer: str | None = Field(None, alias="deviceManufacturer")
    device_hardware_version: str | None = Field(None, alias="deviceHardwareVersion")
    device_software_version: str | None = Field(None, alias="deviceSoftwareVersion")
    metadata: dict[str, str] = Field(default_factory=dict)

    @field_validator("metadata", mode="before")
    @classmethod
    def normalize_metadata(cls, value: object) -> dict[str, str]:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise ValueError("metadata must be an object")
        return {str(key): str(item) for key, item in value.items()}

    def sqlite_row(self, batch_index: int) -> tuple:
        """Return insert tuple matching the HealthKit SQLite schema."""
        return (
            self.uuid,
            self.type.value,
            self.start,
            self.end,
            self.value,
            self.unit,
            self.source,
            self.device_name,
            self.device_model,
            self.device_manufacturer,
            self.device_hardware_version,
            self.device_software_version,
            json.dumps(self.metadata, ensure_ascii=False, sort_keys=True),
            batch_index,
        )


class HealthKitBatch(BaseModel):
    """POST body for /ios-dump from the iOS HealthKit export app."""

    model_config = _FROZEN

    batch_index: int = Field(alias="batchIndex")
    samples: list[HealthKitSample] = Field(default_factory=list)
