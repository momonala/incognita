from dataclasses import dataclass


@dataclass()
class GeoCoords(object):
    lat: float
    lon: float

    def as_tuple(self):
        return self.lat, self.lon


@dataclass(init=False)
class GeoBoundingBox(object):
    center: GeoCoords
    sw: GeoCoords
    ne: GeoCoords
    name: str

    def __init__(self, center: GeoCoords, width: float = 0.05, name=""):
        self.center = center
        self.sw = GeoCoords(center.lat + width, center.lon + width)
        self.ne = GeoCoords(center.lat - width, center.lon - width)
        self.name = name
