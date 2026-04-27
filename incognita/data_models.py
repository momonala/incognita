from dataclasses import dataclass
from datetime import datetime


@dataclass
class GeoCoords:
    """Latitude and longitude (WGS84)."""

    lat: float
    lon: float


@dataclass(init=False)
class GeoBoundingBox:
    center: GeoCoords
    sw: GeoCoords
    ne: GeoCoords
    name: str

    def __init__(self, center: GeoCoords, width: float = 0.05, name: str = ""):
        self.center = center
        # sw = min(lat, lon); ne = max(lat, lon)
        self.sw = GeoCoords(center.lat - width, center.lon - width)
        self.ne = GeoCoords(center.lat + width, center.lon + width)
        self.name = name


@dataclass(frozen=True)
class Country:
    """Country record compatible with the pycountry Country interface, for non-ISO entries."""

    alpha_2: str
    alpha_3: str
    name: str
    flag: str


@dataclass(frozen=True)
class LiveLocationSnapshot:
    """Most recent GPS fix and the full day's simplified trip paths."""

    lat: float
    lon: float
    timestamp: datetime  # timezone-aware UTC
    day_paths: list[list[list[float]]]  # per trip: [[lon, lat, unix_ts_sec], ...]


@dataclass(frozen=True)
class TripDisplayStats:
    """Display stats for the GPS trips map (no raw GPS point count)."""

    track_points: int  # Vertices in simplified trip paths
    trips_count: int
