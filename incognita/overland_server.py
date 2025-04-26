"""Basic HTTP server to receive and store GPS raw_data from iPhone Overland app."""

import logging
import random
import string
import time
from datetime import datetime

import pandas as pd
import requests
from flask import Flask, jsonify, request
import threading

from incognita.database import fetch_coordinates, update_db
from incognita.processing import add_speed_to_gdf
from incognita.utils import get_ip_address
from incognita.values import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID


logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

overland_port = 5003

# Heartbeat timeout settings (seconds)
HEARTBEAT_TIMEOUT = 60 * 3
last_heartbeat = datetime.now()


def send_telegram_alert():
    """Send alert message with current backoff status."""
    message = f"🪦 No heartbeat in last {round(HEARTBEAT_TIMEOUT/60, 1)} minutes!\nLast received at {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}"
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
    global last_heartbeat, HEARTBEAT_TIMEOUT
    logger.info("Watchdog started")
    while True:
        time.sleep(HEARTBEAT_TIMEOUT)
        now = datetime.now()
        if (now - last_heartbeat).total_seconds() > HEARTBEAT_TIMEOUT:
            send_telegram_alert()
            # avoid spamming multiple alerts - double timeout, but not more than 1 hour
            HEARTBEAT_TIMEOUT *= 2
            HEARTBEAT_TIMEOUT = min(HEARTBEAT_TIMEOUT, 60 * 60)
        else:
            # reset timeout to 3 minutes if heartbeat is received
            HEARTBEAT_TIMEOUT = 60 * 3


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    global last_heartbeat
    last_heartbeat = datetime.now()
    return jsonify({"status": "ok"}), 200


@app.route("/dump", methods=["POST"])
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
