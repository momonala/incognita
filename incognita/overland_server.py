"""Basic HTTP server to receive and store GPS raw_data from iPhone Overland app."""

import logging
import random
import string
import time
from datetime import datetime, timedelta
from functools import wraps

import pandas as pd
import requests
from flask import Flask, jsonify, request, Response
import threading

from incognita.database import fetch_coordinates, update_db
from incognita.processing import add_speed_to_gdf
from incognita.utils import get_ip_address
from incognita.values import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID


logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

app = Flask(__name__)

overland_port = 5003

# Heartbeat timeout settings (seconds)
HEARTBEAT_TIMEOUT_DEFAULT = 60
HEARTBEAT_TIMEOUT = HEARTBEAT_TIMEOUT_DEFAULT
last_heartbeat = datetime.now()
is_heartbeat_down = False  # Track heartbeat state
last_alert_time = None  # Track when we last sent an alert
heartbeat_down_time = None  # Track when heartbeat first went down


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
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    
    try:
        requests.post(url, json=payload, timeout=10)
        logger.info(f"{message}. Telegram alert sent.")
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")


@app.route("/", methods=["GET"])
def root():
    return jsonify({"status": "ok"})


@app.route("/status", methods=["GET"])
def status():
    return jsonify({"status": "ok"})


def watchdog():
    global last_heartbeat, HEARTBEAT_TIMEOUT, is_heartbeat_down, last_alert_time, heartbeat_down_time
    logger.info("Watchdog started")
    
    while True:
        time.sleep(1)  # Check every second
        now = datetime.now()
        time_since_heartbeat = (now - last_heartbeat).total_seconds()
        
        # Check if heartbeat is down
        if time_since_heartbeat > HEARTBEAT_TIMEOUT:
            # State changed to down
            if not is_heartbeat_down:
                is_heartbeat_down = True
                heartbeat_down_time = now - timedelta(seconds=HEARTBEAT_TIMEOUT)
                last_alert_time = now
                downtime = (now - heartbeat_down_time).total_seconds()
                message = f"🪦 No heartbeat for {format_downtime(downtime)}!\nLast received at {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}"
                send_telegram_alert(message)
            # Send follow-up alert if enough time has passed
            elif last_alert_time and (now - last_alert_time).total_seconds() >= HEARTBEAT_TIMEOUT:
                downtime = (now - heartbeat_down_time).total_seconds()
                last_alert_time = now
                message = f"🪦 No heartbeat for {format_downtime(downtime)}!\nLast received at {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}"
                send_telegram_alert(message)
                # Increase timeout for next alert
                HEARTBEAT_TIMEOUT = min(HEARTBEAT_TIMEOUT * 2, 60 * 60)
        else:
            if is_heartbeat_down:  # State changed to up
                downtime = (now - heartbeat_down_time).total_seconds()
                message = f"💚 Heartbeat recovered!\nDowntime: {format_downtime(downtime)}\nLast heartbeat: {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}"
                send_telegram_alert(message)
                is_heartbeat_down = False
                heartbeat_down_time = None
                last_alert_time = None
                HEARTBEAT_TIMEOUT = HEARTBEAT_TIMEOUT_DEFAULT  # Reset timeout


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
    logger.info(f"Starting Watchdog Heartbeat timer.")
    logger.info(f"Timeout: {HEARTBEAT_TIMEOUT} seconds.")
    threading.Thread(target=watchdog, daemon=True).start()
    logger.info(f"Running server at http://{get_ip_address()}:{overland_port}")
    app.run(host="0.0.0.0", port=overland_port)
