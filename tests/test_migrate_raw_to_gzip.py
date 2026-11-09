import gzip
import json

import pytest

from incognita.scripts.migrate_raw_to_gzip import Outcome
from incognita.scripts.migrate_raw_to_gzip import migrate_file

PAYLOAD = {"locations": [{"properties": {"timestamp": "2024-01-01T12:00:00Z", "note": "café"}}]}


def _write_plain(path, payload=PAYLOAD):
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def test_migrate_file_replaces_plain_with_compact_gzip(tmp_path):
    source = _write_plain(tmp_path / "a.geojson")
    mtime_ns = source.stat().st_mtime_ns

    result = migrate_file(source)

    target = tmp_path / "a.geojson.gz"
    assert result.outcome is Outcome.CONVERTED
    assert not source.exists()
    raw = gzip.decompress(target.read_bytes()).decode("utf-8")
    assert json.loads(raw) == PAYLOAD
    assert "\n" not in raw
    assert target.stat().st_mtime_ns == mtime_ns
    assert not list(tmp_path.glob("*.tmp"))


def test_migrate_file_finishes_a_half_done_run_and_is_safe_to_rerun(tmp_path):
    source = _write_plain(tmp_path / "a.geojson")
    (tmp_path / "a.geojson.gz").write_bytes(gzip.compress(json.dumps(PAYLOAD).encode()))

    result = migrate_file(source)

    assert result.outcome is Outcome.ALREADY_DONE
    assert not source.exists()


@pytest.mark.parametrize(
    "existing_gz",
    [
        gzip.compress(b'{"locations": []}'),
        gzip.compress(b'{"locations": []}' * 50)[:10] + b"\xff" * 30,
    ],
    ids=["different content", "corrupt deflate stream"],
)
def test_migrate_file_keeps_original_when_existing_gz_is_wrong(tmp_path, existing_gz):
    source = _write_plain(tmp_path / "a.geojson")
    (tmp_path / "a.geojson.gz").write_bytes(existing_gz)

    result = migrate_file(source)

    assert result.outcome is Outcome.FAILED
    assert source.exists()


def test_migrate_file_keeps_unreadable_original(tmp_path):
    source = tmp_path / "bad.geojson"
    source.write_text("not json")

    result = migrate_file(source)

    assert result.outcome is Outcome.FAILED
    assert source.exists()
    assert not (tmp_path / "bad.geojson.gz").exists()


def test_migrate_file_dry_run_changes_nothing(tmp_path):
    source = _write_plain(tmp_path / "a.geojson")

    result = migrate_file(source, dry_run=True)

    assert result.outcome is Outcome.CONVERTED
    assert source.exists()
    assert not (tmp_path / "a.geojson.gz").exists()
