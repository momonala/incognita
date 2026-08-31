"""Tests for the /gps map route."""

from incognita.app import app
from incognita.data_models import TripDisplayStats

EMPTY_STATS = TripDisplayStats(track_points=0, trips_count=0)


def _stub_renderers(monkeypatch, paths, stats):
    """Point the route at canned trip data and capture what it asks the map to render."""
    rendered: list[list] = []

    def _fake_render(paths_for_map, filename, bbox):
        rendered.append(paths_for_map)
        filename.write_text("<html></html>")

    monkeypatch.setattr("incognita.app.get_trip_points_for_date_range", lambda s, e: None)
    monkeypatch.setattr("incognita.app.get_trips_for_date_range", lambda s, e: (paths, stats))
    monkeypatch.setattr("incognita.app.render_trips_to_file", _fake_render)
    return rendered


def _post_range(start="2025-01-01", end="2025-01-02"):
    with app.test_client() as client:
        return client.post("/gps", data={"start_date": start, "end_date": end})


def test_gps_route_rerenders_map_for_empty_range(monkeypatch):
    """An empty range must overwrite the map file, not leave the previous range's on screen."""
    rendered = _stub_renderers(monkeypatch, None, EMPTY_STATS)

    response = _post_range()

    assert response.status_code == 200
    assert rendered == [[]]


def test_gps_route_renders_paths_for_populated_range(monkeypatch):
    paths = [[[0.0, 0.0], [0.0, 0.01]]]
    rendered = _stub_renderers(monkeypatch, paths, TripDisplayStats(track_points=2, trips_count=1))

    response = _post_range()

    assert response.status_code == 200
    assert rendered == [paths]


def test_gps_route_versions_map_url(monkeypatch):
    """The map path is fixed, so the iframe URL carries a version to defeat caching."""
    _stub_renderers(monkeypatch, None, EMPTY_STATS)

    response = _post_range()

    assert b"gps_pydeck.html?v=" in response.data
