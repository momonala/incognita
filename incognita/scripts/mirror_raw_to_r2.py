import logging
import os
import subprocess
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Annotated

import typer

from incognita.database import RAW_DATA_ROOT
from incognita.database import find_geojson_files

logger = logging.getLogger(__name__)

R2_DESTINATION = "r2:incognita/raw"
STATE_FILE = (
    Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
    / "incognita-r2-mirror"
    / "last_success"
)


def find_files_changed_since(root: Path, since: datetime) -> list[Path]:
    """Return raw gzipped GeoJSON files modified at or after `since`, as paths relative to root."""
    cutoff = since.timestamp()
    return [path.relative_to(root) for path in find_geojson_files(root) if path.stat().st_mtime >= cutoff]


def read_last_success(state_file: Path) -> datetime | None:
    try:
        return datetime.fromisoformat(state_file.read_text().strip())
    except FileNotFoundError:
        return None


def write_last_success(state_file: Path, when: datetime) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    temp = state_file.with_name(state_file.name + ".tmp")
    temp.write_text(when.isoformat())
    os.replace(temp, state_file)


def upload(root: Path, files: list[Path], destination: str) -> None:
    """Copy files (relative to root) to the R2 destination. Never deletes anything remotely."""
    upload_timeout_s = 30 * 60
    # No --no-traverse: the r2 remote sets no_head_object, so rclone would assume every file
    # already exists and --immutable would reject new uploads.
    subprocess.run(
        [
            "rclone",
            "copy",
            str(root),
            destination,
            "--files-from-raw",
            "-",
            "--immutable",
            "--size-only",
            "--retries",
            "5",
            "--low-level-retries",
            "20",
        ],
        input="".join(f"{path.as_posix()}\n" for path in files),
        text=True,
        check=True,
        timeout=upload_timeout_s,
    )


def main(
    root: Annotated[Path, typer.Option(help="Raw data directory to mirror.")] = RAW_DATA_ROOT,
    destination: Annotated[str, typer.Option(help="rclone destination.")] = R2_DESTINATION,
    state_file: Annotated[
        Path, typer.Option(help="Where the last successful run time is kept.")
    ] = STATE_FILE,
    since: Annotated[
        datetime | None,
        typer.Option(
            help="Upload files modified since this local time instead of since the last successful run. "
            "Required on the first run."
        ),
    ] = None,
) -> None:
    """Upload every raw file written since the last successful run to R2, then record this run's start time."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")
    started = datetime.now(UTC)
    window_start = since.astimezone() if since else read_last_success(state_file)
    if window_start is None:
        logger.error("No previous run recorded in %s. Pass --since to choose where to start.", state_file)
        raise typer.Exit(code=1)

    # A file keeps the mtime of its temp file, so one renamed into place while the last run was
    # scanning can be slightly older than that run's start.
    rescan_overlap = timedelta(minutes=15)
    files = find_files_changed_since(root, window_start - rescan_overlap)
    logger.info("%d file(s) modified since %s", len(files), window_start.isoformat())
    if files:
        try:
            upload(root, files, destination)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.error("Upload failed, last successful run stays at %s: %s", window_start.isoformat(), e)
            raise typer.Exit(code=1) from e

    write_last_success(state_file, started)
    logger.info("Uploaded %d file(s) to %s", len(files), destination)


if __name__ == "__main__":
    typer.run(main)
