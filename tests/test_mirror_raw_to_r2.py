import os
import subprocess
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
import typer

from incognita.scripts.mirror_raw_to_r2 import find_files_changed_since
from incognita.scripts.mirror_raw_to_r2 import main
from incognita.scripts.mirror_raw_to_r2 import read_last_success
from incognita.scripts.mirror_raw_to_r2 import write_last_success

NOW = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)


def _file(root: Path, relative: str, modified: datetime) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")
    os.utime(path, (modified.timestamp(), modified.timestamp()))
    return path


class FakeRclone:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.calls: list[tuple[list[str], str]] = []

    def __call__(self, args, *, input, **_kwargs):
        self.calls.append((args, input))
        if self.fail:
            raise subprocess.CalledProcessError(returncode=1, cmd=args)


@pytest.fixture
def rclone(monkeypatch):
    fake = FakeRclone()
    monkeypatch.setattr("incognita.scripts.mirror_raw_to_r2.subprocess.run", fake)
    return fake


def test_find_files_changed_since_includes_late_arrivals_in_old_days(tmp_path):
    since = NOW - timedelta(hours=1)
    _file(tmp_path, "2026/11/01/10/old.geojson.gz", NOW - timedelta(days=8))
    _file(tmp_path, "2026/11/09/11/new.geojson.gz", NOW - timedelta(minutes=10))
    _file(tmp_path, "2026/11/02/08/late.geojson.gz", NOW - timedelta(minutes=5))
    _file(tmp_path, "2026/11/09/11/new.geojson.gz.abc.tmp", NOW)
    _file(tmp_path, ".DS_Store", NOW)

    found = find_files_changed_since(tmp_path, since)

    assert found == [Path("2026/11/02/08/late.geojson.gz"), Path("2026/11/09/11/new.geojson.gz")]


def test_main_uploads_new_files_and_advances_the_marker(tmp_path, rclone):
    state_file = tmp_path / "state" / "last_success"
    root = tmp_path / "raw"
    write_last_success(state_file, datetime.now(UTC) - timedelta(hours=1))
    _file(root, "2026/11/09/11/new.geojson.gz", datetime.now(UTC))
    before = datetime.now(UTC)

    main(root=root, destination="r2:bucket/raw", state_file=state_file, since=None)

    ((args, stdin),) = rclone.calls
    assert args[:4] == ["rclone", "copy", str(root), "r2:bucket/raw"]
    assert "sync" not in args
    assert stdin == "2026/11/09/11/new.geojson.gz\n"
    assert read_last_success(state_file) >= before


def test_main_rescans_files_renamed_into_place_just_before_the_last_run(tmp_path, rclone):
    state_file = tmp_path / "last_success"
    last_run = datetime.now(UTC) - timedelta(minutes=30)
    write_last_success(state_file, last_run)
    _file(tmp_path / "raw", "2026/11/09/11/raced.geojson.gz", last_run - timedelta(minutes=1))

    main(root=tmp_path / "raw", destination="r2:bucket/raw", state_file=state_file, since=None)

    ((_args, stdin),) = rclone.calls
    assert stdin == "2026/11/09/11/raced.geojson.gz\n"


def test_main_with_nothing_new_skips_upload_and_advances_the_marker(tmp_path, rclone):
    state_file = tmp_path / "last_success"
    previous = datetime.now(UTC) - timedelta(hours=1)
    write_last_success(state_file, previous)

    main(root=tmp_path, destination="r2:bucket/raw", state_file=state_file, since=None)

    assert rclone.calls == []
    assert read_last_success(state_file) > previous


def test_main_keeps_the_marker_when_the_upload_fails(tmp_path, monkeypatch):
    monkeypatch.setattr("incognita.scripts.mirror_raw_to_r2.subprocess.run", FakeRclone(fail=True))
    state_file = tmp_path / "last_success"
    previous = datetime.now(UTC) - timedelta(hours=1)
    write_last_success(state_file, previous)
    _file(tmp_path / "raw", "2026/11/09/11/new.geojson.gz", datetime.now(UTC))

    with pytest.raises(typer.Exit) as exc:
        main(root=tmp_path / "raw", destination="r2:bucket/raw", state_file=state_file, since=None)

    assert exc.value.exit_code == 1
    assert read_last_success(state_file) == previous


def test_main_refuses_to_guess_a_start_point_on_first_run(tmp_path, rclone):
    with pytest.raises(typer.Exit) as exc:
        main(root=tmp_path, destination="r2:bucket/raw", state_file=tmp_path / "missing", since=None)

    assert exc.value.exit_code == 1
    assert rclone.calls == []
