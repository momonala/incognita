"""Basic HTTP server to receive and store GPS raw_data from iPhone Overland app."""
import logging
import random
import signal
import string
import subprocess
import time

from flask import Flask, request, jsonify

from incognita.database import update_db
from incognita.utils import get_ip_address

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

PORT = 8383
static_url = "full-primarily-weevil.ngrok-free.app"
ngrok_process: subprocess.Popen | None = None

app = Flask(__name__)


@app.route('/', methods=['GET'])
def home():
    return jsonify({"result": "ok"})


@app.route('/dump', methods=['POST'])
def dump():
    """Receive and store GPS GeoJSON raw_data from iPhone."""
    data = request.get_data()
    rand = "".join(random.sample(string.ascii_lowercase, 7))
    file_name = f'raw_data/{time.strftime("%Y%m%d-%H%M%S")}-{rand}.geojson'

    with open(file_name, "w") as fh:
        fh.write(data.decode())
    logging.info(f"Wrote {file_name=}")

    update_db(file_name)
    return jsonify({"result": "ok"})


def start_ngrok():
    global ngrok_process
    try:
        cmd = f"ngrok http --domain={static_url} {PORT}".split(" ")
        logger.info(f"Starting ngrok {cmd=}")
        ngrok_process = subprocess.Popen(cmd)
        logger.info(f"Started ngrok {ngrok_process.pid=}")
    except:
        logger.warning("not able to start ngrok")


def stop_ngrok():
    global ngrok_process
    if ngrok_process is not None:
        ngrok_process.terminate()
        ngrok_process.wait(timeout=5)  # Wait for the process to terminate


def sigterm_handler(signo, frame):
    """Register a signal handler to stop Ngrok when Flask is killed"""
    stop_ngrok()
    exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, sigterm_handler)
    start_ngrok()

    logger.info(f"Running server at http://{get_ip_address()}:{PORT}/store")
    app.run(host='0.0.0.0', port=PORT)
