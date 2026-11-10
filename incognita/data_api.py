"""Basic HTTP server to receive and store GPS incognita_raw_data from iPhone Overland app."""

import gc
import hashlib
import json
import logging
import os
import shutil
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from functools import wraps
from pathlib import Path
from typing import Protocol

import psutil
import requests
import schedule
from flask import Flask
from flask import Response
from flask import jsonify
from flask import redirect
from flask import request
from pydantic import ValidationError

from incognita.config import DASHBOARD_PORT
from incognita.config import SPYGLASS_DASHBOARD_URL
from incognita.data_models import DailyMotionStats
from incognita.data_models import HealthDump
from incognita.data_models import HealthDumpRange
from incognita.data_models import HealthKitBatch
from incognita.data_models import MotionStatsRange
from incognita.database import DB_FILE
from incognita.database import RAW_DATA_ROOT
from incognita.database import find_geojson_files
from incognita.database import pack_geojson
from incognita.database import update_db
from incognita.database import write_geojson_file
from incognita.env import TELEGRAM_CHAT_ID
from incognita.env import TELEGRAM_TOKEN
from incognita.gps_trips_renderer import get_trip_points_for_date_range
from incognita.health_database import HEALTH_DB_FILE
from incognita.health_database import get_daily_health_dump
from incognita.health_database import get_health_dump_range
from incognita.health_database import insert_health_batch
from incognita.motion_stats import get_daily_motion_stats
from incognita.motion_stats import get_motion_stats_range
from incognita.observability import configure_logging
from incognita.observability import metrics
from incognita.utils import BYTES_PER_MB
from incognita.utils import get_ip_address

logger = logging.getLogger(__name__)

app = Flask(__name__)

overland_port = 5003

DEFAULT_LOOKBACK_HOURS = 24
MAX_STATS_LOOKBACK_DAYS = 366
TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%SZ"

ALERT_SCHEDULE_SEC = (60, 300, 600, 3600)


def format_downtime(seconds: float) -> str:
    """Format seconds into human readable format based on duration."""
    td = timedelta(seconds=int(seconds))
    days = td.days
    hours = td.seconds // 3600
    minutes = (td.seconds % 3600) // 60
    seconds = td.seconds % 60

    if days > 0:
        return f"{days}d, {hours}h, {minutes}m, {seconds}s"
    elif hours > 0:
        return f"{hours}h, {minutes}m, {seconds}s"
    else:
        return f"{minutes}m, {seconds}s"


def log_payload_size(f):
    """Decorator to log the size of the response payload in MB."""

    @wraps(f)
    def decorated_function(*args, **kwargs):
        response = f(*args, **kwargs)
        if isinstance(response, Response):
            payload_size = len(response.get_data())
            payload_size_mb = payload_size / 1024 / 1024
            logger.debug("Response payload size: %.6f MB", payload_size_mb)
        return response

    return decorated_function


class Notifier(Protocol):
    def send(self, message: str) -> int | None: ...

    def delete(self, message_id: int) -> None: ...


class TelegramNotifier:
    def send(self, message: str) -> int | None:
        """Send a Telegram message, returning its message_id (or None on failure)."""
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
        try:
            response = requests.post(url, json=payload, timeout=10)
            response_data = response.json()
            if response_data.get("ok") and "result" in response_data:
                return response_data["result"]["message_id"]
            logger.error(f"Failed to send Telegram message: {response_data}")
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
        return None

    def delete(self, message_id: int) -> None:
        delete_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "message_id": message_id}
        try:
            response = requests.post(delete_url, json=payload, timeout=10)
            if response.json().get("ok"):
                logger.debug(f"Deleted previous message {message_id}")
            else:
                logger.warning(f"Failed to delete previous message {message_id}: {response.json()}")
        except Exception as e:
            logger.error(f"Error deleting previous message {message_id}: {e}")


@dataclass(frozen=True)
class Up:
    pass


@dataclass(frozen=True)
class Down:
    level: int


@dataclass(frozen=True)
class Snoozed:
    until: datetime
    level: int


WatchdogState = Up | Down | Snoozed


def alert_threshold_sec(level: int) -> int:
    """Downtime at which the alert for `level` fires: the schedule, then every further hour."""
    if level < len(ALERT_SCHEDULE_SEC):
        return ALERT_SCHEDULE_SEC[level]
    return ALERT_SCHEDULE_SEC[-1] + 3600 * (level - len(ALERT_SCHEDULE_SEC) + 1)


class Watchdog:
    """Heartbeat watchdog: escalating downtime alerts, snoozable, one live alert message in the chat.

    `level` is the number of alerts raised since the last recovery; the next one fires at
    `alert_threshold_sec(level)`. Recovery happens on the first tick that sees the heartbeat
    younger than the first threshold. A snooze keeps the level until the first tick inside the
    snooze window, which drops it to 0.
    """

    def __init__(self, notifier: Notifier, clock: Callable[[], datetime] = datetime.now):
        self._notifier = notifier
        self._clock = clock
        self._lock = threading.Lock()
        self._state: WatchdogState = Up()
        self._last_heartbeat = clock()
        self._last_message_id: int | None = None

    @property
    def state(self) -> WatchdogState:
        with self._lock:
            return self._state

    @property
    def last_heartbeat(self) -> datetime:
        with self._lock:
            return self._last_heartbeat

    def muted_reason(self, now: datetime) -> str | None:
        """Why alerts are suppressed at `now` (active snooze or quiet hours, 23:00–06:59), else None."""
        with self._lock:
            state = self._state
        if isinstance(state, Snoozed) and now < state.until:
            return f"snoozed until {state.until:%H:%M}"
        if now.hour >= 23 or now.hour < 7:
            return "sleepy time"
        return None

    def on_heartbeat(self) -> None:
        with self._lock:
            self._last_heartbeat = self._clock()

    def snooze(self, minutes: int) -> datetime:
        with self._lock:
            until = self._clock() + timedelta(minutes=minutes)
            self._state = Snoozed(until, self._state.level if isinstance(self._state, Down | Snoozed) else 0)
        duration = f"{minutes // 60}h{minutes % 60:02d}m"
        logger.info(f"Alerts snoozed for {duration}, until {until:%Y-%m-%d %H:%M:%S}")
        self._notifier.send(f"😴 Alerts snoozed for {duration}, until {until:%H:%M}")
        return until

    def on_tick(self) -> None:
        now = self._clock()
        with self._lock:
            alert = self._advance(now)
        if alert is not None:
            self._send_alert(alert)

    def _advance(self, now: datetime) -> str | None:
        match self._state:
            case Snoozed(until) if now < until:
                self._state = Snoozed(until, 0)
                return None
            case Snoozed(level=level) if level > 0:
                self._state = Down(level)
            case Snoozed():
                self._state = Up()

        level = self._state.level if isinstance(self._state, Down) else 0
        downtime_sec = int((now - self._last_heartbeat).total_seconds())
        logger.debug(
            f"Downtime(s): {downtime_sec:<10} Next Alert(s): {alert_threshold_sec(level):<10} {self._state=}"
        )

        if downtime_sec >= alert_threshold_sec(level):
            self._state = Down(level + 1)
            return (
                f"🪦 No heartbeat for {format_downtime(downtime_sec)}!\n"
                f"Last received at {self._last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        if downtime_sec < ALERT_SCHEDULE_SEC[0] and isinstance(self._state, Down):
            self._state = Up()
        return None

    def _send_alert(self, message: str) -> None:
        """Send an alert, deleting the previous one so the chat shows a single live status."""
        if reason := self.muted_reason(self._clock()):
            logger.debug(f"Skipping alert ({reason})")
            return

        with self._lock:
            previous_id, self._last_message_id = self._last_message_id, None
        if previous_id is not None:
            self._notifier.delete(previous_id)

        message_id = self._notifier.send(message)
        if message_id is not None:
            with self._lock:
                self._last_message_id = message_id
            logger.debug(f"{message}. Telegram alert sent with ID: {message_id}")


heartbeat_watchdog = Watchdog(TelegramNotifier())


@app.route("/", methods=["GET"])
def root():
    return jsonify({"status": "ok"})


@app.route("/status", methods=["GET"])
def status():
    return jsonify({"status": "ok"})


@app.route("/observability", methods=["GET"])
def observability():
    """Redirect to the Spyglass-hosted observability dashboard."""
    return redirect(SPYGLASS_DASHBOARD_URL)


@app.before_request
def _spyglass_request_start():
    """Record request start time for API latency metrics."""
    request.environ["_spyglass_start"] = time.perf_counter()


_TRACKED_ENDPOINTS = {"dump", "ios_dump"}


@app.after_request
def _spyglass_request_end(response):
    """Emit per-endpoint request latency."""
    endpoint = request.endpoint or "unknown"
    if endpoint in _TRACKED_ENDPOINTS:
        elapsed_ms = (
            time.perf_counter() - request.environ.get("_spyglass_start", time.perf_counter())
        ) * 1000
        metrics.timing(f"api.{endpoint}.latency_ms", elapsed_ms)
    return response


def _count_files(root: Path) -> int:
    """Recursively count files under root via scandir (avoids per-entry stat calls, unlike glob/Path.is_file)."""
    count = 0
    dirs = [root]
    while dirs:
        with os.scandir(dirs.pop()) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    dirs.append(entry.path)
                else:
                    count += 1
    return count


def report_storage_metrics() -> None:
    """Emit raw-data file count and total DB size (MB) to spyglass. Scheduled hourly from main()."""
    file_count = _count_files(RAW_DATA_ROOT)
    db_size_mb = (
        sum(
            p.stat().st_size
            for db_file in (DB_FILE, HEALTH_DB_FILE)
            for p in Path(db_file).parent.glob(f"{Path(db_file).name}*")
        )
        / BYTES_PER_MB
    )
    metrics.gauge("raw_data_file_count", file_count)
    metrics.gauge("db_size_mb", db_size_mb)


_JOBLIB_CACHE_ROOT = Path(".cache/incognita/gps_trips_renderer/_get_month_trips_cached_impl")
_JOBLIB_CACHE_MAX_ENTRIES = 24


def _evict_old_cache_entries() -> None:
    """Keep only the N most-recently-accessed joblib cache entries."""
    if not _JOBLIB_CACHE_ROOT.exists():
        return
    entries = sorted(_JOBLIB_CACHE_ROOT.iterdir(), key=lambda p: p.stat().st_atime, reverse=True)
    for old_entry in entries[_JOBLIB_CACHE_MAX_ENTRIES:]:
        shutil.rmtree(old_entry, ignore_errors=True)


def report_process_metrics() -> None:
    """Emit process heap metrics to spyglass. Scheduled hourly from main().

    The two gauges are only meaningful read together. A rising ``rss_mb`` has two very
    different causes: real object retention, which moves ``gc_objects`` with it, and
    allocator fragmentation, where freed memory is never returned to the OS and
    ``gc_objects`` stays flat. Splitting those is the whole point of sampling both.

    ``gc.get_objects()`` materialises a list of every tracked object, so cost scales
    with heap size: ~3ms at 142k objects on the import graph alone. Cheap hourly,
    but re-measure before sampling it anywhere near per-request.
    """
    rss_mb = psutil.Process().memory_info().rss / BYTES_PER_MB
    gc_objects = len(gc.get_objects())
    metrics.gauge("rss_mb", rss_mb)
    metrics.gauge("gc_objects", gc_objects)


def metrics_scheduler():
    schedule.every().hour.do(report_storage_metrics)
    schedule.every().hour.do(report_process_metrics)
    schedule.every().hour.do(_evict_old_cache_entries)
    report_storage_metrics()
    report_process_metrics()
    _evict_old_cache_entries()
    while True:
        schedule.run_pending()
        time.sleep(60)


def run_watchdog():
    logger.info(f"Watchdog started with {ALERT_SCHEDULE_SEC=}")
    while True:
        time.sleep(1)
        heartbeat_watchdog.on_tick()


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    heartbeat_watchdog.on_heartbeat()
    try:
        requests.post(f"http://localhost:{DASHBOARD_PORT}/internal/heartbeat", timeout=1)
    except Exception as e:
        logger.warning(f"Failed to send heartbeat to dashboard: {e}")
    return jsonify({"status": "ok"}), 200


@app.route("/snooze", methods=["POST"])
def snooze():
    """Mute downtime alerts for N minutes (1–1440) or N hours (1–24). Used when the phone is intentionally offline."""
    body = request.get_json(silent=True) or {}
    try:
        minutes = int(body["minutes"]) if "minutes" in body else int(body.get("hours")) * 60
    except (TypeError, ValueError):
        return jsonify({"error": "send minutes (1–1440) or hours (1–24) as an integer"}), 400
    if not 1 <= minutes <= 24 * 60:
        return jsonify({"error": "snooze must be between 1 minute and 24 hours"}), 400

    snooze_until = heartbeat_watchdog.snooze(minutes)
    return jsonify({"status": "ok", "snooze_until": snooze_until.isoformat()}), 200


def get_hour(ts):
    """Extract year, month, day, hour, minute from ISO timestamp."""
    # Parse ISO timestamp (handles 'Z' timezone)
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return str(dt.year), f"{dt.month:02d}", f"{dt.day:02d}", f"{dt.hour:02d}", f"{dt.minute:02d}"


def get_content_hash(features):
    """Generate fast deterministic hash from features - hash first/last timestamp + count."""
    if not features:
        return "0000000"
    # Hash first timestamp, last timestamp, and count for speed
    first_ts = features[0]["properties"]["timestamp"]
    last_ts = features[-1]["properties"]["timestamp"]
    count = len(features)
    hash_input = f"{first_ts}|{last_ts}|{count}".encode()
    return hashlib.md5(hash_input).hexdigest()[:7]


def _log_dump_target_diagnostics(
    target_path: Path, file_name: Path, locations_count: int, wrote_file: bool
) -> None:
    """Log dump write metadata needed to correlate ingestion with coordinate cache keys."""
    day_path = target_path.parent
    day_stat = day_path.stat()
    hour_stat = target_path.stat()
    day_geojson_files = len(find_geojson_files(day_path))
    logger.debug(
        "[dump-cache-diagnostics] wrote_file=%s file=%s locations=%s day_dir=%s day_size=%s "
        "day_mtime_ns=%s hour_dir=%s hour_size=%s hour_mtime_ns=%s day_geojson_files=%s",
        wrote_file,
        file_name,
        locations_count,
        day_path,
        day_stat.st_size,
        day_stat.st_mtime_ns,
        target_path,
        hour_stat.st_size,
        hour_stat.st_mtime_ns,
        day_geojson_files,
    )


def _get_coordinates_window(lookback_hours: int) -> tuple[datetime, datetime]:
    """Return the UTC window used by the coordinates endpoints."""
    end_dt = datetime.now(UTC)
    start_dt = end_dt - timedelta(hours=lookback_hours)
    return start_dt, end_dt


def _get_day_window(date_str: str) -> tuple[datetime, datetime]:
    """Return the UTC window covering one local calendar day, clamped to now.

    Day boundaries are local (matching /motion-stats and /health-data), but the window is
    converted to UTC because the raw data directories are partitioned by UTC hour.
    """
    day = datetime.strptime(date_str, "%Y-%m-%d")
    start_local = day.astimezone()
    end_local = (day + timedelta(days=1)).astimezone()
    now = datetime.now(UTC)
    return start_local.astimezone(UTC), min(end_local.astimezone(UTC), now)


def _format_ts_for_api(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).strftime(TIMESTAMP_FMT)


def _trip_points_to_api_paths(start_dt: datetime, end_dt: datetime) -> list[list[dict[str, float | str]]]:
    """Return segmented simplified trip paths for the requested window.

    Keep trips separated so clients can render one polyline per trip and avoid
    drawing straight jumps across telemetry gaps.
    """
    trip_points = get_trip_points_for_date_range(start_dt, end_dt) or []
    return [
        [
            {
                "timestamp": _format_ts_for_api(ts),
                "latitude": lat,
                "longitude": lon,
            }
            for lon, lat, ts in path
        ]
        for path in trip_points
    ]


@app.route("/dump", methods=["POST"])
@log_payload_size
def dump():
    """Receive and store GPS GeoJSON incognita_raw_data/ from iPhone."""
    data = request.get_data()

    # Parse and format JSON with indentation
    json_data = json.loads(data.decode())
    locations = json_data.get("locations", [])
    metrics.increment("files_received")
    metrics.gauge("locations_count", len(locations))

    # Get timestamp from first location to determine directory structure
    first_timestamp = locations[0]["properties"]["timestamp"]
    year, month, day, hour, minute = get_hour(first_timestamp)

    # Create directory structure: incognita_raw_data/YYYY/MM/DD/HH/
    target_path = Path("incognita_raw_data") / year / month / day / hour
    target_path.mkdir(parents=True, exist_ok=True)

    # Generate deterministic filename using content hash
    content_hash = get_content_hash(locations)
    file_name = target_path / f"{year}{month}{day}-{hour}{minute}00-{content_hash}.geojson.gz"

    # Only write if file doesn't exist (avoid duplicates)
    if not file_name.exists():
        write_geojson_file(file_name, pack_geojson(json_data))
        logger.debug("Wrote file_name=%s", file_name)
        update_db(str(file_name))
        wrote_file = True
        metrics.increment("wrote_file")
    else:
        logging.warning(f"File already exists, skipping: {file_name=}")
        wrote_file = False
        metrics.increment("duplicate_skipped")
    _log_dump_target_diagnostics(target_path, file_name, len(locations), wrote_file)
    return jsonify({"result": "ok"})


@app.route("/ios-dump", methods=["POST"])
@log_payload_size
def ios_dump():
    """Receive and store raw HealthKit samples from the iOS export app."""
    try:
        batch = HealthKitBatch.model_validate_json(request.get_data())
    except ValidationError as e:
        metrics.increment("validation_error")
        return jsonify({"result": "error", "message": str(e)}), 400

    metrics.increment("batches_received")
    inserted, skipped = insert_health_batch(batch)
    logger.debug(
        "ios-dump batch_index=%s inserted=%s skipped=%s",
        batch.batch_index,
        inserted,
        skipped,
    )
    return jsonify({"result": "ok", "inserted": inserted, "skipped": skipped})


def _parse_lookback_days(raw: str | None, *, default: int = 7) -> tuple[int | None, tuple[dict, int] | None]:
    """Parse and validate a lookback-days query param. Returns (days, error_response)."""
    if raw is None:
        return default, None
    try:
        days = int(raw)
    except (TypeError, ValueError):
        return None, ({"status": "error", "message": "days must be an integer between 1 and 366"}, 400)
    if not 1 <= days <= MAX_STATS_LOOKBACK_DAYS:
        return None, (
            {"status": "error", "message": f"days must be between 1 and {MAX_STATS_LOOKBACK_DAYS}"},
            400,
        )
    return days, None


@app.route("/motion-stats", methods=["GET"])
def motion_stats():
    """Return today's GPS motion summary from the location database.

    Loads all points for the requested calendar day (local date by default). Moving stats use
    rows with speed > 0; ``motion_type.stationary`` time uses rows labeled ``stationary`` (distance always 0).
    Distance and time are summed from consecutive point segments (haversine); altitude
    ascended/descended sum positive/negative deltas between consecutive readings; speeds use
    per-row Overland speed values (m/s).

    Query params:
        date (str): YYYY-MM-DD or ``today``. Defaults to today (local time).
    """
    # extract and validate date
    date_str = request.args.get("date") or datetime.now().strftime("%Y-%m-%d")
    if date_str == "today":
        date_str = datetime.now().strftime("%Y-%m-%d")
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return (
            jsonify(
                {"status": "error", "message": "Invalid date format: please provide date as YYYY-MM-DD."}
            ),
            400,
        )

    stats = DailyMotionStats.model_validate(get_daily_motion_stats(date_str))
    return jsonify(stats.model_dump(mode="json")), 200


@app.route("/motion-stats-range", methods=["GET"])
def motion_stats_range():
    """Return daily GPS motion summaries for the last N calendar days ending today.

    Query params:
        days (int): Number of days to include, 1–366. Defaults to 7.
    """
    days, error = _parse_lookback_days(request.args.get("days"))
    if error:
        body, status = error
        return jsonify(body), status

    stats = [DailyMotionStats.model_validate(row) for row in get_motion_stats_range(days)]
    payload = MotionStatsRange(days=days, stats=stats)
    return jsonify(payload.model_dump(mode="json")), 200


@app.route("/health-data", methods=["GET"])
def health_data():
    """Return a daily health summary aggregated from the HealthKit database.

    Query params:
        date (str): YYYY-MM-DD. Defaults to today (local time).

    Returns JSON matching the HealthDump schema.
    """
    date_str = request.args.get("date") or datetime.now().strftime("%Y-%m-%d")
    if date_str == "today":
        date_str = datetime.now().strftime("%Y-%m-%d")

    totals = get_daily_health_dump(date_str)
    dump = HealthDump(
        date=date_str,
        steps=int(totals["steps"]) if totals["steps"] is not None else None,
        kcals=totals["kcals"],
        km=totals["km"],
        flights_climbed=int(totals["flights_climbed"]) if totals["flights_climbed"] is not None else None,
        weight=None,
        recorded_at=datetime.now(),
    )
    return jsonify(dump.model_dump(mode="json")), 200


@app.route("/health-data-range", methods=["GET"])
def health_data_range():
    """Return daily health summaries for the last N calendar days ending today.

    Query params:
        days (int): Number of days to include, 1–366. Defaults to 7.
    """
    days, error = _parse_lookback_days(request.args.get("days"))
    if error:
        body, status = error
        return jsonify(body), status

    health_rows = get_health_dump_range(days)
    health = [
        HealthDump(
            date=row["date"],
            steps=int(row["steps"]) if row["steps"] is not None else None,
            kcals=row["kcals"],
            km=row["km"],
            flights_climbed=int(row["flights_climbed"]) if row["flights_climbed"] is not None else None,
            weight=None,
            recorded_at=datetime.now(),
        )
        for row in health_rows
    ]
    payload = HealthDumpRange(days=days, health=health)
    return jsonify(payload.model_dump(mode="json")), 200


@app.route("/coordinates", methods=["GET"])
@log_payload_size
def get_coordinates():
    """Return simplified trip coordinates from raw GPS files.

    Query params (mutually exclusive):
        date (str): YYYY-MM-DD or ``today``. Returns that local calendar day.
        lookback_hours (int): Hours back from now. Defaults to 24.
    """
    date_arg = request.args.get("date")
    lookback_arg = request.args.get("lookback_hours")
    if date_arg is not None and lookback_arg is not None:
        return jsonify({"status": "error", "message": "pass either date or lookback_hours, not both"}), 400

    try:
        if date_arg is not None:
            date_str = datetime.now().strftime("%Y-%m-%d") if date_arg == "today" else date_arg
            try:
                start_dt, end_dt = _get_day_window(date_str)
            except ValueError:
                return (
                    jsonify(
                        {
                            "status": "error",
                            "message": "Invalid date format: please provide date as YYYY-MM-DD.",
                        }
                    ),
                    400,
                )
            # A day that hasn't started yet has no window to query.
            if start_dt >= end_dt:
                return jsonify({"status": "success", "count": 0, "date": date_str, "paths": []})
            scope: dict[str, str | int] = {"date": date_str}
        else:
            try:
                lookback_hours = int(lookback_arg) if lookback_arg is not None else DEFAULT_LOOKBACK_HOURS
            except ValueError:
                return jsonify({"status": "error", "message": "lookback_hours must be an integer"}), 400
            if lookback_hours <= 0:
                return jsonify({"status": "error", "message": "lookback_hours must be positive"}), 400
            start_dt, end_dt = _get_coordinates_window(lookback_hours)
            scope = {"lookback_hours": lookback_hours}

        logger.debug(
            "[coordinates] fetching file-backed coordinates scope=%s start=%s end=%s",
            scope,
            start_dt.isoformat(),
            end_dt.isoformat(),
        )
        paths = _trip_points_to_api_paths(start_dt, end_dt)
        coordinate_count = sum(len(path) for path in paths)
        logger.debug(
            "[coordinates] response paths=%s coordinates=%s scope=%s start=%s end=%s",
            len(paths),
            coordinate_count,
            scope,
            start_dt.isoformat(),
            end_dt.isoformat(),
        )
        return jsonify({"status": "success", "count": coordinate_count, **scope, "paths": paths})

    except Exception as e:
        logger.error(f"Error fetching coordinates: {str(e)}")
        return jsonify({"status": "error", "message": f"Failed to fetch coordinates: {str(e)}"}), 500


def main():
    configure_logging()
    threading.Thread(target=run_watchdog, daemon=True).start()
    threading.Thread(target=metrics_scheduler, daemon=True).start()
    logger.info(f"Running server at http://{get_ip_address()}:{overland_port}")
    app.run(host="0.0.0.0", port=overland_port)


if __name__ == "__main__":
    main()
