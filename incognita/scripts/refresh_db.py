import logging
import os
from glob import glob

from tqdm import tqdm

from incognita.database import update_db, create_timestamp_index

logger = logging.getLogger("incognita.database")
logger.setLevel(logging.WARNING)

DB_FILE = "cache/geo_data.db"

if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

geo_files = sorted(glob("raw_data/*.geojson"))
for f in tqdm(geo_files):
    update_db(f, DB_FILE)
create_timestamp_index()
