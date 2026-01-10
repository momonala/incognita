import logging
import os
import sqlite3
from multiprocessing import Pool, cpu_count
from pathlib import Path

from tqdm import tqdm

from incognita.database import DB_FILE, DB_NAME, create_timestamp_index, update_db

logger = logging.getLogger("incognita.database")
logger.setLevel(logging.WARNING)

root_dir = Path("raw_data")


def process_file(file_path):
    """Process a single file and update the database."""
    try:
        update_db(str(file_path), DB_FILE)
        return True
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return False


def main():
    db_file = str(DB_FILE)

    if os.path.exists(db_file):
        os.remove(db_file)

    # Initialize database with WAL mode and create table upfront
    with sqlite3.connect(db_file) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")

        # Create table upfront to avoid race conditions in parallel processing
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DB_NAME} (
                lon REAL,
                lat REAL,
                timestamp TEXT,
                speed REAL,
                altitude REAL,
                horizontal_accuracy REAL,
                motion TEXT,
                geojson_file TEXT
            )
        """
        )
        conn.commit()

    geo_files = sorted(root_dir.rglob("*.geojson"))
    print(f"Found {len(geo_files):,} files to process")

    # Process files in parallel
    # Each worker will create its own connection (SQLite WAL mode handles concurrent writes)
    num_workers = max(1, cpu_count() - 1)
    print(f"Using {num_workers} workers")

    with Pool(num_workers) as pool:
        results = list(tqdm(pool.imap(process_file, geo_files), total=len(geo_files), desc="Updating DB"))

    # Create index after all data is loaded
    create_timestamp_index()

    # Vacuum database to reclaim space and optimize
    print("Vacuuming database...")
    with sqlite3.connect(db_file) as conn:
        conn.execute("VACUUM")
        conn.commit()

    success_count = sum(results)
    print(f"Successfully processed {success_count:,}/{len(geo_files):,} files")

    # Final stats
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {DB_NAME}")
        final_count = cursor.fetchone()[0]
        db_size_mb = Path(db_file).stat().st_size / (1024 * 1024)
        print(f"Final database: {final_count:,} rows, {db_size_mb:.2f} MB")


if __name__ == "__main__":
    main()
