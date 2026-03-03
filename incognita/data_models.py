from dataclasses import dataclass


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
class TripDisplayStats:
    """Display stats for the GPS trips map (no raw GPS point count)."""

    track_points: int  # Vertices in simplified trip paths
    trips_count: int
