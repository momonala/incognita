import logging
import subprocess

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

ssh_tunnel_process = None
port = 5003


def start_ssh_tunnel():
    global ssh_tunnel_process
    static_url = "full-primarily-weevil.ngrok-free.app"
    cmd = f"ngrok http --domain={static_url} {port}"
    cmd = f"lt --port {port} --subdomain incognita"
    logger.info(f"Starting ssh tunnel: {cmd=}")

    try:
        ssh_tunnel_process = subprocess.Popen(cmd.split(" "))
        logger.info(f"Started ssh tunnel: {ssh_tunnel_process.pid=}")
        logger.info(f"ipv4: {requests.get('https://ipv4.icanhazip.com/').text.strip()}")
    except Exception as e:
        logger.warning(f"not able to start ssh tunnel: {e}")


def stop_ssh_tunnel():
    global ssh_tunnel_process
    if ssh_tunnel_process is not None:
        ssh_tunnel_process.terminate()
        ssh_tunnel_process.wait(timeout=5)


def sigterm_handler(signo, frame):
    """Register a signal handler to stop SSH Tunnel when Flask is killed"""
    stop_ssh_tunnel()
    exit(0)
