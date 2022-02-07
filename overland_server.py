"""Basic HTTP server to receive and store GPS raw_data from iPhone Overland app."""
import logging
import random
import socket
import string
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)
PORT = 8383


class StoreHandler(BaseHTTPRequestHandler):
    """In overland the url will be http://yourservername_or_ip:8383/store"""
    def do_POST(self):
        """Recieve and store GPS GeoJSON raw_data from iPhone."""
        if self.path == "/store":
            length = self.headers["content-length"]
            data = self.rfile.read(int(length))
            rand = "".join(random.sample(string.ascii_lowercase, 7))
            file_name = f'raw_data/{time.strftime("%Y%m%d-%H%M%S")}-{rand}.geojson'

            with open(file_name, "w") as fh:
                fh.write(data.decode())
            logging.info(f"Wrote {file_name=}")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"result": "ok"}')


def get_ip_address() -> str:
    """Get the IP address of the current server."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 100))
    socket_name = s.getsockname()
    s.close()
    return socket_name[0]


if __name__ == "__main__":
    server = HTTPServer(("", PORT), StoreHandler)
    ip_address = get_ip_address()
    logger.info(f"Running server at http://{ip_address}:{PORT}/store")
    server.serve_forever()
