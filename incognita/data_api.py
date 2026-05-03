"""Basic HTTP server to receive and store GPS incognita_raw_data from iPhone Overland app."""

import bisect
import hashlib
import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, request

from incognita.config import DASHBOARD_PORT
from incognita.database import update_db
from incognita.gps_trips_renderer import get_trip_points_for_date_range
from incognita.utils import get_ip_address
from incognita.values import TELEGRAM_CHAT_ID, TELEGRAM_TOKEN

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

app = Flask(__name__)

overland_port = 5003
last_heartbeat = datetime.now()

DEFAULT_LOOKBACK_HOURS = 24
TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%SZ"

# Global variable to store the last sent Telegram message ID
last_message_id: int | None = None
last_message_lock = threading.Lock()


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
            logger.info(f"Response payload size: {payload_size_mb:.6f} MB")
        return response

    return decorated_function


def send_telegram_alert(message: str):
    """Send alert message with current backoff status. Deletes previous alert except for 'Heartbeat recovered'."""
    global last_message_id
    # if time between 11pm and 7am, don't send alerts
    if datetime.now().hour < 7 or datetime.now().hour > 23:
        logger.info("🌙 Skipping alert because it's sleepy time!")
        return

    # If this is a heartbeat recovered message, reset last_message_id and do not delete anything
    if "recovered" in message.lower():
        with last_message_lock:
            last_message_id = None
        logger.info("Heartbeat recovered message sent. Not deleting any previous message.")
    else:
        # Delete the previous message if it exists
        with last_message_lock:
            if last_message_id is not None:
                delete_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteMessage"
                payload = {"chat_id": TELEGRAM_CHAT_ID, "message_id": last_message_id}
                try:
                    response = requests.post(delete_url, json=payload, timeout=10)
                    if response.json().get("ok"):
                        logger.debug(f"🗑️ Deleted previous message {last_message_id}")
                    else:
                        logger.warning(
                            f"Failed to delete previous message {last_message_id}: {response.json()}"
                        )
                except Exception as e:
                    logger.error(f"Error deleting previous message {last_message_id}: {e}")
                last_message_id = None

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}

    try:
        response = requests.post(url, json=payload, timeout=10)
        response_data = response.json()
        if response_data.get("ok") and "result" in response_data:
            message_id = response_data["result"]["message_id"]
            with last_message_lock:
                last_message_id = message_id
            logger.info(f"{message}. Telegram alert sent with ID: {message_id}")
        else:
            logger.error(f"Failed to send Telegram message: {response_data}")
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")


@app.route("/", methods=["GET"])
def root():
    return jsonify({"status": "ok"})


@app.route("/status", methods=["GET"])
def status():
    return jsonify({"status": "ok"})


def watchdog():
    # Schedule in seconds: 1m, 5m, 30m, 60m
    alert_schedule = [60, 300, 600, 3600]
    next_alert = alert_schedule[0]
    is_down = False
    logger.info(f"Watchdog started with {alert_schedule=}")

    def _get_next_alert(current: int) -> int:
        # Get next alert time by cycling through schedule
        if current < alert_schedule[-1]:
            idx = bisect.bisect_right(alert_schedule, current)
            return alert_schedule[idx] if idx < len(alert_schedule) else current + 3600
        return current + 3600

    while True:
        time.sleep(1)
        now = datetime.now()
        heartbeat_down_time = now - last_heartbeat
        downtime_sec = int(heartbeat_down_time.total_seconds())
        logger.debug(f"Downtime(s): {downtime_sec:<10} Next Alert(s): {next_alert:<10} {is_down=}")

        if downtime_sec >= next_alert:
            message = (
                f"🪦 No heartbeat for {format_downtime(downtime_sec)}!\n"
                f"Last received at {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            send_telegram_alert(message)
            is_down = True
            next_alert = _get_next_alert(next_alert)

        elif downtime_sec < alert_schedule[0] and is_down:
            message = (
                f"💚 Heartbeat recovered!\n"
                f"Downtime: {format_downtime(downtime_sec)}\n"
                f"Last heartbeat: {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            send_telegram_alert(message)
            next_alert = alert_schedule[0]
            is_down = False


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    global last_heartbeat
    last_heartbeat = datetime.now()
    try:
        requests.post(f"http://localhost:{DASHBOARD_PORT}/internal/heartbeat", timeout=1)
    except Exception as e:
        logger.warning(f"Failed to send heartbeat to dashboard: {e}")
    return jsonify({"status": "ok"}), 200


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
    day_geojson_files = sum(1 for _ in day_path.rglob("*.geojson"))
    logger.info(
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
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=lookback_hours)
    return start_dt, end_dt


def _format_ts_for_api(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(TIMESTAMP_FMT)


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

    # Get timestamp from first location to determine directory structure
    first_timestamp = locations[0]["properties"]["timestamp"]
    year, month, day, hour, minute = get_hour(first_timestamp)

    # Create directory structure: incognita_raw_data/YYYY/MM/DD/HH/
    target_path = Path("incognita_raw_data") / year / month / day / hour
    target_path.mkdir(parents=True, exist_ok=True)

    # Generate deterministic filename using content hash
    content_hash = get_content_hash(locations)
    file_name = target_path / f"{year}{month}{day}-{hour}{minute}00-{content_hash}.geojson"

    # Only write if file doesn't exist (avoid duplicates)
    if not file_name.exists():
        with open(file_name, "w") as fh:
            json.dump(json_data, fh, indent=2, ensure_ascii=False)
        logging.info(f"Wrote {file_name=}")
        update_db(str(file_name))
        wrote_file = True
    else:
        logging.warning(f"File already exists, skipping: {file_name=}")
        wrote_file = False
    _log_dump_target_diagnostics(target_path, file_name, len(locations), wrote_file)
    return jsonify({"result": "ok"})


@app.route("/coordinates", methods=["GET"])
@log_payload_size
def get_coordinates():
    """Return simplified trip coordinates from raw GPS files."""
    try:
        lookback_hours = request.args.get("lookback_hours", default=DEFAULT_LOOKBACK_HOURS, type=int)
        if lookback_hours <= 0:
            return jsonify({"status": "error", "message": "lookback_hours must be positive"}), 400

        start_dt, end_dt = _get_coordinates_window(lookback_hours)
        logger.info(
            "[coordinates] fetching file-backed coordinates lookback_hours=%s start=%s end=%s",
            lookback_hours,
            start_dt.isoformat(),
            end_dt.isoformat(),
        )
        paths = _trip_points_to_api_paths(start_dt, end_dt)
        coordinate_count = sum(len(path) for path in paths)
        logger.info(
            "[coordinates] response paths=%s coordinates=%s lookback_hours=%s start=%s end=%s",
            len(paths),
            coordinate_count,
            lookback_hours,
            start_dt.isoformat(),
            end_dt.isoformat(),
        )
        return jsonify(
            {
                "status": "success",
                "count": coordinate_count,
                "lookback_hours": lookback_hours,
                "paths": paths,
            }
        )

    except Exception as e:
        logger.error(f"Error fetching coordinates: {str(e)}")
        return jsonify({"status": "error", "message": f"Failed to fetch coordinates: {str(e)}"}), 500


def main():
    threading.Thread(target=watchdog, daemon=True).start()
    logger.info(f"Running server at http://{get_ip_address()}:{overland_port}")
    app.run(host="0.0.0.0", port=overland_port)


if __name__ == "__main__":
    main()
