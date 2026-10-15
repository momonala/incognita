"""Tests for the /live/health stats bar route."""

from incognita.app import app

EMPTY_TOTALS = {"steps": None, "kcals": None, "km": None, "flights_climbed": None}


def _get_live_health(monkeypatch, totals):
    monkeypatch.setattr("incognita.app.get_daily_health_dump", lambda date: totals)
    with app.test_client() as client:
        return client.get("/live/health")


def test_live_health_returns_todays_totals(monkeypatch):
    totals = {"steps": 8421.0, "kcals": 512.3, "km": 6.2, "flights_climbed": 4.0}

    response = _get_live_health(monkeypatch, totals)

    assert response.status_code == 200
    assert response.get_json() == {"data": [totals]}


def test_live_health_returns_empty_when_no_samples_today(monkeypatch):
    response = _get_live_health(monkeypatch, EMPTY_TOTALS)

    assert response.get_json() == {"data": []}
