import gzip
import json
import logging
import os
import zlib
from concurrent.futures import ProcessPoolExecutor
from enum import StrEnum
from functools import partial
from pathlib import Path
from typing import Annotated

import typer
from pydantic import BaseModel

from incognita.database import RAW_DATA_ROOT
from incognita.database import pack_geojson
from incognita.database import write_geojson_file

logger = logging.getLogger(__name__)


class Outcome(StrEnum):
    CONVERTED = "converted"
    ALREADY_DONE = "already_done"
    FAILED = "failed"


class FileResult(BaseModel):
    outcome: Outcome
    bytes_before: int = 0
    bytes_after: int = 0
    error: str | None = None


def migrate_file(source: Path, dry_run: bool = False) -> FileResult:
    """Replace one plain `.geojson` file with a compact `.geojson.gz`, deleting the original only after the copy round-trips.

    A `.gz` that already holds identical content (a run that died before the delete) just has the
    original removed. Anything unreadable is left untouched and reported.
    """
    target = source.with_name(source.name + ".gz")
    try:
        stat = source.stat()
        parsed = json.loads(source.read_text(encoding="utf-8"))
        if target.exists():
            if json.loads(gzip.decompress(target.read_bytes())) != parsed:
                return FileResult(
                    outcome=Outcome.FAILED, error=f"{target.name} exists with different content"
                )
            if not dry_run:
                source.unlink()
            return FileResult(
                outcome=Outcome.ALREADY_DONE, bytes_before=stat.st_size, bytes_after=target.stat().st_size
            )

        packed = pack_geojson(parsed)
        if json.loads(gzip.decompress(packed)) != parsed:
            return FileResult(outcome=Outcome.FAILED, error="round-trip mismatch")
        if not dry_run:
            write_geojson_file(target, packed)
            os.utime(target, ns=(stat.st_atime_ns, stat.st_mtime_ns))
            source.unlink()
        return FileResult(outcome=Outcome.CONVERTED, bytes_before=stat.st_size, bytes_after=len(packed))
    except (OSError, ValueError, EOFError, zlib.error) as e:
        return FileResult(outcome=Outcome.FAILED, error=f"{type(e).__name__}: {e}")


def main(
    root: Annotated[Path, typer.Option(help="Raw data directory to migrate.")] = RAW_DATA_ROOT,
    dry_run: Annotated[bool, typer.Option(help="Verify every file but change nothing.")] = False,
    workers: Annotated[
        int,
        typer.Option(
            help="Parallel worker processes. Each file waits on an fsync, so more than the core count helps."
        ),
    ] = 16,
) -> None:
    """Convert every plain `.geojson` under ROOT to compact `.geojson.gz` and remove the original."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")
    sources = sorted(root.rglob("*.geojson"))
    logger.info(
        "Found %d plain .geojson files under %s%s", len(sources), root, " (dry run)" if dry_run else ""
    )

    progress_every = 10_000
    counts = {outcome: 0 for outcome in Outcome}
    bytes_before = bytes_after = 0
    failed: list[tuple[Path, str | None]] = []
    with ProcessPoolExecutor(max_workers=workers) as pool:
        results = pool.map(partial(migrate_file, dry_run=dry_run), sources, chunksize=500)
        for done, (source, result) in enumerate(zip(sources, results, strict=True), 1):
            counts[result.outcome] += 1
            bytes_before += result.bytes_before
            bytes_after += result.bytes_after
            if result.outcome is Outcome.FAILED:
                failed.append((source, result.error))
            if done % progress_every == 0:
                logger.info("%d/%d files processed", done, len(sources))

    logger.info(
        "converted=%d already_done=%d failed=%d | %.2f GB -> %.2f GB",
        counts[Outcome.CONVERTED],
        counts[Outcome.ALREADY_DONE],
        counts[Outcome.FAILED],
        bytes_before / 1e9,
        bytes_after / 1e9,
    )
    for source, error in failed:
        logger.error("left in place: %s (%s)", source, error)
    if failed:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    typer.run(main)
