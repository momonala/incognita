"""Basic HTTP server to receive and store GPS raw_data from iPhone Overland app."""
import logging
import random
import string
import time

from flask import Flask, request, jsonify

from incognita.database import update_db
from incognita.ssh_tunnel import port as overland_port
from incognita.utils import get_ip_address

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route('/', methods=['GET'])
def root():
    return jsonify({"status": "ok"})


@app.route('/status', methods=['GET'])
def status():
    return jsonify({"status": "ok"})


@app.route('/dump', methods=['POST'])
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


if __name__ == "__main__":
    logger.info(f"Running server at http://{get_ip_address()}:{overland_port}/dump")
    app.run(host='0.0.0.0', port=overland_port)
