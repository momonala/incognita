import pytest
from flask.testing import FlaskClient

from incognita import data_api

TEST_TOKEN = "test-trace-token"


class _AuthorizedClient(FlaskClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.environ_base["HTTP_AUTHORIZATION"] = f"Bearer {TEST_TOKEN}"


@pytest.fixture(autouse=True)
def authorized_data_api(monkeypatch):
    """`data_api.app.test_client()` sends the right token; build a bare `FlaskClient(app)` to test without it."""
    monkeypatch.setattr(data_api, "TRACE_API_TOKEN", TEST_TOKEN)
    monkeypatch.setattr(data_api.app, "test_client_class", _AuthorizedClient)
