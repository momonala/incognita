import socket

import pandas as pd

from incognita.data_models import GeoBoundingBox, GeoCoords

BYTES_PER_MB = 1024 * 1024

# Hardcoded default map center (Berlin). Used for GPS page and as fallback when airport/city is unknown.
BERLIN_LAT = 52.52
BERLIN_LON = 13.405
DEFAULT_MAP_BOX = GeoBoundingBox(center=GeoCoords(BERLIN_LAT, BERLIN_LON), width=0.065, name="Berlin")


def get_ip_address() -> str:
    """Get the IP address of the current server."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 100))
    socket_name = s.getsockname()
    s.close()
    return socket_name[0]


GOOGLE_SHEETS_DOCUMENT_ID = "1V4hVhSH1_tHizwqlSQ2ymysQwwQMuFENfE9lB5vJPQY"


def google_sheets_export_csv_url(sheet_name: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_DOCUMENT_ID}/gviz/tq?tqx=out:csv&sheet={sheet_name}"


def google_sheets_document_url() -> str:
    """URL to open the Google Sheets document in the browser (no export params)."""
    return f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_DOCUMENT_ID}/edit"


def read_google_sheets_csv(export_url: str) -> pd.DataFrame:
    return pd.read_csv(export_url, keep_default_na=False)
