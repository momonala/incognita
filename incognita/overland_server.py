"""Basic HTTP server to receive and store GPS raw_data from iPhone Overland app."""

import logging
import random
import string
import threading
import time
from datetime import datetime, timedelta
from functools import wraps

import pandas as pd
from flask import Flask, Response, jsonify, request

from incognita.database import fetch_coordinates, update_db
from incognita.processing import add_speed_to_gdf
from incognita.utils import get_ip_address
from incognita.telegram import update_daily_summary

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

app = Flask(__name__)

overland_port = 5003
last_heartbeat = datetime.now()


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

    while True:
        time.sleep(1)
        now = datetime.now()
        heartbeat_down_time = now - last_heartbeat
        downtime_sec = int(heartbeat_down_time.total_seconds())
        
        # Log every 30 seconds to see what's happening
        if downtime_sec % 30 == 0:
            logger.debug(f"Watchdog status - Downtime: {downtime_sec}s, Next alert: {next_alert}s, Is down: {is_down}")
        
        if downtime_sec >= next_alert:
            logger.debug(f"🚨 Alert triggered! Downtime: {downtime_sec}s, Next alert was: {next_alert}s")
            # Track heartbeat loss event
            if not is_down:  # Only track when first going down
                logger.debug("📱 Sending heartbeat lost event to daily summary")
                update_daily_summary("lost", last_heartbeat)
            is_down = True

            # Get next alert time by cycling through schedule
            current_idx = alert_schedule.index(next_alert)
            next_alert = alert_schedule[(current_idx + 1) % len(alert_schedule)]
            logger.debug(f"Next alert scheduled for: {next_alert}s")

        elif downtime_sec < alert_schedule[0] and is_down:
            logger.info(f"💚 Heartbeat recovered! Downtime was: {downtime_sec}s")
            # Track heartbeat recovery event
            update_daily_summary("recovered", last_heartbeat, downtime_sec)
            next_alert = alert_schedule[0]
            is_down = False


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    global last_heartbeat
    last_heartbeat = datetime.now()
    logger.debug(f"💓 Heartbeat received at {last_heartbeat.strftime('%H:%M:%S')}")
    return jsonify({"status": "ok"}), 200


@app.route("/test-heartbeat-lost", methods=["POST"])
def test_heartbeat_lost():
    """Test endpoint to simulate a heartbeat loss event."""
    global last_heartbeat
    # Set last_heartbeat to 2 minutes ago to trigger the alert
    last_heartbeat = datetime.now() - timedelta(minutes=2)
    logger.info("🧪 Test: Set last_heartbeat to 2 minutes ago to trigger alert")
    return jsonify({"status": "test_triggered", "last_heartbeat": last_heartbeat.isoformat()}), 200


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
