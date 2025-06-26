"""Basic HTTP server to receive and store GPS raw_data from iPhone Overland app."""

import logging
import random
import string
import threading
import time
from datetime import datetime, timedelta
from functools import wraps

import pandas as pd
import requests
from flask import Flask, Response, jsonify, request

from incognita.database import fetch_coordinates, update_db
from incognita.processing import add_speed_to_gdf
from incognita.utils import get_ip_address
from incognita.values import TELEGRAM_CHAT_ID, TELEGRAM_TOKEN

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

app = Flask(__name__)

overland_port = 5003
last_heartbeat = datetime.now()

# Global list to store message IDs and their timestamps for cleanup
message_cleanup_queue: list[tuple[int, datetime]] = []
message_cleanup_lock = threading.Lock()


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
    """Send alert message with current backoff status."""
    # if time between 11pm and 7am, don't send alerts
    if datetime.now().hour < 7 or datetime.now().hour > 23:
        logger.info("🌙 Skipping alert because it's sleepy time!")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}

    try:
        response = requests.post(url, json=payload, timeout=10)
        response_data = response.json()
        
        if response_data.get("ok") and "result" in response_data:
            message_id = response_data["result"]["message_id"]
            with message_cleanup_lock:
                message_cleanup_queue.append((message_id, datetime.now()))
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
    is_down = False
    current_message_id = None
    last_update = datetime.now()
    logger.info("Watchdog started with minute-based updates")

    def _update_message(message_id: int, message: str):
        """Update existing message silently."""
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "message_id": message_id,
            "text": message
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.json().get("ok"):
                logger.debug(f"📝 Updated message {message_id}")
            else:
                logger.warning(f"Failed to update message {message_id}: {response.json()}")
        except Exception as e:
            logger.error(f"Error updating message {message_id}: {e}")

    def _delete_message(message_id: int):
        """Delete message and remove from cleanup queue."""
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "message_id": message_id}
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.json().get("ok"):
                logger.info(f"🗑️ Deleted message {message_id}")
                # Remove from cleanup queue if present
                with message_cleanup_lock:
                    message_cleanup_queue[:] = [(mid, ts) for mid, ts in message_cleanup_queue if mid != message_id]
            else:
                logger.warning(f"Failed to delete message {message_id}: {response.json()}")
        except Exception as e:
            logger.error(f"Error deleting message {message_id}: {e}")

    while True:
        time.sleep(1)
        now = datetime.now()
        heartbeat_down_time = now - last_heartbeat
        downtime_sec = int(heartbeat_down_time.total_seconds())
        logger.debug(f"Downtime(s): {downtime_sec:<10} {is_down=}")

        if downtime_sec >= 60:  # Alert after 1 minute of downtime
            if not is_down:
                # First time going down - create new message
                message = (
                    f"🪦 No heartbeat for {format_downtime(downtime_sec)}!\n"
                    f"Last received at {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                send_telegram_alert(message)
                # Get the message ID from the last sent message
                with message_cleanup_lock:
                    if message_cleanup_queue:
                        current_message_id = message_cleanup_queue[-1][0]
                is_down = True
                last_update = now
            else:
                # Already down - update existing message every minute
                if (now - last_update).total_seconds() >= 60:
                    message = (
                        f"🪦 No heartbeat for {format_downtime(downtime_sec)}!\n"
                        f"Last received at {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    if current_message_id:
                        _update_message(current_message_id, message)
                    last_update = now

        elif downtime_sec < 60 and is_down:
            # Heartbeat recovered - delete the current message
            if current_message_id:
                _delete_message(current_message_id)
                current_message_id = None
            
            message = (
                f"💚 Heartbeat recovered!\n"
                f"Downtime: {format_downtime(downtime_sec)}\n"
                f"Last heartbeat: {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            send_telegram_alert(message)
            is_down = False


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    global last_heartbeat
    last_heartbeat = datetime.now()
    return jsonify({"status": "ok"}), 200


@app.route("/dump", methods=["POST"])
@log_payload_size
def dump():
    """Receive and store GPS GeoJSON raw_data from iPhone."""
    data = request.get_data()
    rand_id = "".join(random.sample(string.ascii_lowercase, 7))
    file_name = f'raw_data/{time.strftime("%Y%m%d-%H%M%S")}-{rand_id}.geojson'

    with open(file_name, "w") as fh:
        fh.write(data.decode())
    logging.info(f"Wrote {file_name=}")

    update_db(file_name)
    return jsonify({"result": "ok"})


@app.route("/coordinates", methods=["GET"])
@log_payload_size
def get_coordinates():
    """Return list of (timestamp, lat, lon) tuples from database.

    Query Parameters:
        lookback_hours: Optional[int] - number of hours to look back (default: 24)
        min_accuracy: Optional[float] - minimum accuracy in meters (default: 200)
        max_distance: Optional[float] - maximum distance in kilometers (default: 0.1)
    """
    try:
        # Get lookback hours from query params, default to 24 if not provided
        lookback_hours = request.args.get("lookback_hours", default=24, type=int)
        min_accuracy = request.args.get("min_accuracy", default=200, type=float)
        max_distance = request.args.get("max_distance", default=0.1, type=float)

        # Ensure args are positive
        for arg in [lookback_hours, min_accuracy, max_distance]:
            if arg <= 0:
                return jsonify({"status": "error", "message": f"{arg} must be positive"}), 400

        # Fetch coordinates while filtering by accuracy
        logger.info(f"Fetching coordinates with {lookback_hours=} {min_accuracy=} {max_distance=}")
        coordinates = fetch_coordinates(
            lookback_hours=lookback_hours,
            min_accuracy=min_accuracy,
        )
        # Convert to pandas DataFrame, add speed, filter by distance
        coordinates = pd.DataFrame(coordinates, columns=["timestamp", "lat", "lon", "accuracy"])
        coordinates = add_speed_to_gdf(coordinates)
        coordinates = coordinates[coordinates["meters"] <= max_distance]
        coordinates = [
            (row["timestamp"], row["lat"], row["lon"], row["accuracy"]) for _, row in coordinates.iterrows()
        ]

        return jsonify(
            {
                "status": "success",
                "count": len(coordinates),
                "lookback_hours": lookback_hours,
                "min_accuracy": min_accuracy,
                "max_distance": max_distance,
                "coordinates": coordinates,
            }
        )

    except Exception as e:
        logger.error(f"Error fetching coordinates: {str(e)}")
        return jsonify({"status": "error", "message": f"Failed to fetch coordinates: {str(e)}"}), 500


if __name__ == "__main__":
    threading.Thread(target=watchdog, daemon=True).start()
    logger.info(f"Running server at http://{get_ip_address()}:{overland_port}")
    app.run(host="0.0.0.0", port=overland_port)
