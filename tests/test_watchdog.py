"""Characterization tests for the heartbeat watchdog state machine and its endpoints."""

from datetime import datetime
from datetime import timedelta

import pytest

from incognita import data_api
from incognita.data_api import Down
from incognita.data_api import Snoozed
from incognita.data_api import Up
from incognita.data_api import Watchdog
from incognita.data_api import app

NOON = datetime(2025, 1, 1, 12, 0, 0)


class FakeClock:
    def __init__(self, now: datetime):
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def at(self, seconds: float) -> None:
        self.now = NOON + timedelta(seconds=seconds)


class FakeNotifier:
    def __init__(self, fail_sends: bool = False):
        self.sent: list[str] = []
        self.deleted: list[int] = []
        self.fail_sends = fail_sends

    def send(self, message: str) -> int | None:
        self.sent.append(message)
        return None if self.fail_sends else 100 + len(self.sent)

    def delete(self, message_id: int) -> None:
        self.deleted.append(message_id)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(NOON)


@pytest.fixture
def notifier() -> FakeNotifier:
    return FakeNotifier()


@pytest.fixture
def watchdog(clock, notifier) -> Watchdog:
    return Watchdog(notifier, clock=clock)


def tick_at(watchdog: Watchdog, clock: FakeClock, seconds: float) -> None:
    clock.at(seconds)
    watchdog.on_tick()


THRESHOLDS = (60, 300, 600, 3600, 7200, 10800, 14400)


def drive_to_level(watchdog: Watchdog, clock: FakeClock, level: int) -> None:
    for threshold in THRESHOLDS[:level]:
        tick_at(watchdog, clock, threshold)


def test_starts_up_with_heartbeat_at_construction(watchdog):
    assert watchdog.state == Up()
    assert watchdog.last_heartbeat == NOON


def test_first_alert_fires_at_exactly_60_seconds_with_exact_text(watchdog, clock, notifier):
    tick_at(watchdog, clock, 59)
    assert notifier.sent == []
    assert watchdog.state == Up()

    tick_at(watchdog, clock, 60)
    assert notifier.sent == ["🪦 No heartbeat for 1m, 0s!\nLast received at 2025-01-01 12:00:00"]
    assert watchdog.state == Down(1)


def test_downtime_is_truncated_to_whole_seconds(watchdog, clock, notifier):
    tick_at(watchdog, clock, 59.999)
    assert notifier.sent == []


@pytest.mark.parametrize("level,threshold", list(enumerate(THRESHOLDS))[1:])
def test_each_escalation_fires_at_its_exact_threshold(watchdog, clock, notifier, level, threshold):
    drive_to_level(watchdog, clock, level)
    sent_before = len(notifier.sent)
    assert watchdog.state == Down(level)

    tick_at(watchdog, clock, threshold - 1)
    assert len(notifier.sent) == sent_before
    assert watchdog.state == Down(level)

    tick_at(watchdog, clock, threshold)
    assert len(notifier.sent) == sent_before + 1
    assert watchdog.state == Down(level + 1)


def test_escalation_text_reports_downtime_and_last_heartbeat(watchdog, clock, notifier):
    drive_to_level(watchdog, clock, 4)
    assert notifier.sent[1:] == [
        "🪦 No heartbeat for 5m, 0s!\nLast received at 2025-01-01 12:00:00",
        "🪦 No heartbeat for 10m, 0s!\nLast received at 2025-01-01 12:00:00",
        "🪦 No heartbeat for 1h, 0m, 0s!\nLast received at 2025-01-01 12:00:00",
    ]


def test_late_tick_fires_one_alert_and_advances_one_level(watchdog, clock, notifier):
    tick_at(watchdog, clock, 700)
    assert notifier.sent == ["🪦 No heartbeat for 11m, 40s!\nLast received at 2025-01-01 12:00:00"]
    assert watchdog.state == Down(1)


def test_each_new_alert_deletes_the_previous_one(watchdog, clock, notifier):
    drive_to_level(watchdog, clock, 3)
    assert notifier.deleted == [101, 102]


def test_failed_send_leaves_nothing_to_delete(clock):
    notifier = FakeNotifier(fail_sends=True)
    watchdog = Watchdog(notifier, clock=clock)
    drive_to_level(watchdog, clock, 2)
    assert len(notifier.sent) == 2
    assert notifier.deleted == []


@pytest.mark.parametrize("level", [1, 2, 3, 4, 5])
def test_recovery_from_each_level_on_next_tick_after_heartbeat(watchdog, clock, notifier, level):
    drive_to_level(watchdog, clock, level)
    sent_before = len(notifier.sent)
    clock.at(8000)
    watchdog.on_heartbeat()
    assert watchdog.state == Down(level)

    tick_at(watchdog, clock, 8001)
    assert watchdog.state == Up()
    assert len(notifier.sent) == sent_before

    tick_at(watchdog, clock, 8059)
    assert len(notifier.sent) == sent_before
    tick_at(watchdog, clock, 8060)
    assert len(notifier.sent) == sent_before + 1
    assert notifier.sent[-1] == "🪦 No heartbeat for 1m, 0s!\nLast received at 2025-01-01 14:13:20"
    assert notifier.deleted[-1] == 100 + sent_before
    assert watchdog.state == Down(1)


def test_recovery_needs_downtime_below_first_threshold(watchdog, clock, notifier):
    drive_to_level(watchdog, clock, 2)
    clock.at(400)
    watchdog.on_heartbeat()

    tick_at(watchdog, clock, 460)
    assert watchdog.state == Down(2)
    tick_at(watchdog, clock, 1000)
    assert len(notifier.sent) == 3
    assert watchdog.state == Down(3)


def test_heartbeat_while_up_keeps_up(watchdog, clock, notifier):
    tick_at(watchdog, clock, 30)
    watchdog.on_heartbeat()
    tick_at(watchdog, clock, 89)
    assert notifier.sent == []
    assert watchdog.state == Up()
    assert watchdog.last_heartbeat == NOON + timedelta(seconds=30)


def test_snooze_start_announces_and_enters_snoozed(watchdog, clock, notifier):
    clock.at(10)
    until = watchdog.snooze(95)
    assert until == NOON + timedelta(seconds=10, minutes=95)
    assert watchdog.state == Snoozed(until, 0)
    assert notifier.sent == ["😴 Alerts snoozed for 1h35m, until 13:35"]
    assert notifier.deleted == []


def test_snooze_suppresses_alerts_until_boundary(watchdog, clock, notifier):
    watchdog.snooze(60)
    notifier.sent.clear()

    tick_at(watchdog, clock, 3599)
    assert notifier.sent == []
    assert watchdog.state == Snoozed(NOON + timedelta(hours=1), 0)

    tick_at(watchdog, clock, 3600)
    assert notifier.sent == ["🪦 No heartbeat for 1h, 0m, 0s!\nLast received at 2025-01-01 12:00:00"]
    assert watchdog.state == Down(1)


def test_snooze_during_down_restarts_escalation_at_first_level(watchdog, clock, notifier):
    drive_to_level(watchdog, clock, 3)
    clock.at(700)
    watchdog.snooze(10)
    assert watchdog.state == Snoozed(NOON + timedelta(seconds=1300), 3)
    tick_at(watchdog, clock, 701)
    assert watchdog.state == Snoozed(NOON + timedelta(seconds=1300), 0)

    sent_before = len(notifier.sent)
    tick_at(watchdog, clock, 1300)
    assert watchdog.state == Down(1)
    tick_at(watchdog, clock, 1301)
    assert watchdog.state == Down(2)
    tick_at(watchdog, clock, 1302)
    assert watchdog.state == Down(3)
    tick_at(watchdog, clock, 1303)
    assert watchdog.state == Down(3)
    assert len(notifier.sent) == sent_before + 3


def test_snooze_message_does_not_replace_the_live_alert(watchdog, clock, notifier):
    tick_at(watchdog, clock, 60)
    clock.at(61)
    watchdog.snooze(1)
    tick_at(watchdog, clock, 62)
    tick_at(watchdog, clock, 121)
    assert notifier.deleted == [101]


def test_heartbeat_during_snooze_keeps_snooze(watchdog, clock, notifier):
    watchdog.snooze(60)
    notifier.sent.clear()
    clock.at(1800)
    watchdog.on_heartbeat()

    tick_at(watchdog, clock, 1801)
    assert watchdog.state == Snoozed(NOON + timedelta(hours=1), 0)
    assert watchdog.muted_reason(clock()) == "snoozed until 13:00"

    tick_at(watchdog, clock, 3599)
    assert notifier.sent == []
    tick_at(watchdog, clock, 3600)
    assert notifier.sent == ["🪦 No heartbeat for 30m, 0s!\nLast received at 2025-01-01 12:30:00"]
    assert watchdog.state == Down(1)


def test_resnooze_replaces_window(watchdog, clock):
    watchdog.snooze(120)
    clock.at(60)
    watchdog.snooze(1)
    assert watchdog.state == Snoozed(NOON + timedelta(minutes=2), 0)


def test_snooze_with_no_tick_inside_window_keeps_escalation_level(watchdog, clock, notifier):
    drive_to_level(watchdog, clock, 3)
    clock.at(700)
    watchdog.snooze(1)
    sent_before = len(notifier.sent)

    for seconds in (770, 771, 772, 3599):
        tick_at(watchdog, clock, seconds)
        assert watchdog.state == Down(3)
    assert len(notifier.sent) == sent_before

    tick_at(watchdog, clock, 3600)
    assert notifier.sent[sent_before:] == [
        "🪦 No heartbeat for 1h, 0m, 0s!\nLast received at 2025-01-01 12:00:00"
    ]
    assert watchdog.state == Down(4)


def test_resnooze_before_any_tick_keeps_escalation_level(watchdog, clock):
    drive_to_level(watchdog, clock, 2)
    clock.at(400)
    watchdog.snooze(1)
    clock.at(401)
    watchdog.snooze(1)
    assert watchdog.state == Snoozed(NOON + timedelta(seconds=461), 2)


def test_muted_reason(watchdog, clock):
    assert watchdog.muted_reason(datetime(2025, 1, 1, 2, 0)) == "sleepy time"
    assert watchdog.muted_reason(datetime(2025, 1, 1, 6, 59)) == "sleepy time"
    assert watchdog.muted_reason(datetime(2025, 1, 1, 7, 0)) is None
    assert watchdog.muted_reason(datetime(2025, 1, 1, 23, 59)) is None

    watchdog.snooze(30)
    assert watchdog.muted_reason(NOON + timedelta(minutes=29)) == "snoozed until 12:30"
    assert watchdog.muted_reason(NOON + timedelta(minutes=30)) is None


def test_quiet_hours_suppress_sending_but_still_escalate():
    clock = FakeClock(datetime(2025, 1, 1, 2, 0, 0))
    notifier = FakeNotifier()
    watchdog = Watchdog(notifier, clock=clock)

    clock.now = datetime(2025, 1, 1, 2, 1, 0)
    watchdog.on_tick()
    assert notifier.sent == []
    assert watchdog.state == Down(1)

    clock.now = datetime(2025, 1, 1, 2, 5, 0)
    watchdog.on_tick()
    assert watchdog.state == Down(2)
    assert notifier.sent == []
    assert notifier.deleted == []


@pytest.fixture
def forwarded(monkeypatch) -> list[str]:
    urls: list[str] = []
    monkeypatch.setattr(data_api.requests, "post", lambda url, **kwargs: urls.append(url))
    return urls


@pytest.fixture
def app_watchdog(monkeypatch, clock, notifier, forwarded) -> Watchdog:
    watchdog = Watchdog(notifier, clock=clock)
    monkeypatch.setattr(data_api, "heartbeat_watchdog", watchdog)
    return watchdog


def test_heartbeat_endpoint_records_heartbeat_and_forwards(app_watchdog, clock, forwarded):
    clock.at(500)
    with app.test_client() as client:
        response = client.post("/heartbeat")

    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}
    assert app_watchdog.last_heartbeat == NOON + timedelta(seconds=500)
    assert forwarded == [f"http://localhost:{data_api.DASHBOARD_PORT}/internal/heartbeat"]


def test_heartbeat_endpoint_survives_dashboard_failure(monkeypatch, app_watchdog, clock):
    def fail(url, **kwargs):
        raise ConnectionError("down")

    monkeypatch.setattr(data_api.requests, "post", fail)
    clock.at(5)
    with app.test_client() as client:
        response = client.post("/heartbeat")

    assert response.status_code == 200
    assert app_watchdog.last_heartbeat == NOON + timedelta(seconds=5)


@pytest.mark.parametrize(
    "body,until,announcement",
    [
        ({"hours": 3}, datetime(2025, 1, 1, 15, 0), "😴 Alerts snoozed for 3h00m, until 15:00"),
        ({"minutes": 95}, datetime(2025, 1, 1, 13, 35), "😴 Alerts snoozed for 1h35m, until 13:35"),
        ({"minutes": 1, "hours": 5}, datetime(2025, 1, 1, 12, 1), "😴 Alerts snoozed for 0h01m, until 12:01"),
        ({"minutes": 1440}, datetime(2025, 1, 2, 12, 0), "😴 Alerts snoozed for 24h00m, until 12:00"),
    ],
)
def test_snooze_endpoint(app_watchdog, notifier, body, until, announcement):
    with app.test_client() as client:
        response = client.post("/snooze", json=body)

    assert response.status_code == 200
    assert response.get_json() == {"status": "ok", "snooze_until": until.isoformat()}
    assert app_watchdog.state == Snoozed(until, 0)
    assert notifier.sent == [announcement]


def test_rejected_snooze_leaves_state_alone(app_watchdog, notifier):
    with app.test_client() as client:
        response = client.post("/snooze", json={"minutes": 0})

    assert response.status_code == 400
    assert response.get_json() == {"error": "snooze must be between 1 minute and 24 hours"}
    assert app_watchdog.state == Up()
    assert notifier.sent == []
