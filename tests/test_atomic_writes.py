import os
import stat
import tarfile
from pathlib import Path
from unittest.mock import Mock
from zipfile import ZipFile

import pandas as pd
import pytest

from app.csv_io import write_csv_atomically


def test_atomic_csv_write_replaces_destination_after_success(tmp_path: Path) -> None:
    # Given an existing CSV destination.
    destination = tmp_path / "profiles.csv"
    destination.write_text("old", encoding="utf-8")

    # When a frame is written atomically.
    write_csv_atomically(pd.DataFrame({"ID": [1, 2]}), destination)

    # Then the complete new CSV replaces the old file without temporary residue.
    assert pd.read_csv(destination)["ID"].tolist() == [1, 2]
    assert set(tmp_path.iterdir()) == {destination}


def test_atomic_csv_write_preserves_compression_inference(tmp_path: Path) -> None:
    # Given a destination whose extension requests gzip compression.
    destination = tmp_path / "profiles.csv.gz"

    # When a frame is written atomically.
    write_csv_atomically(pd.DataFrame({"ID": [1, 2]}), destination)

    # Then pandas can read the result using the destination extension.
    assert pd.read_csv(destination)["ID"].tolist() == [1, 2]


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits")
def test_atomic_csv_write_preserves_existing_permissions(tmp_path: Path) -> None:
    # Given an existing destination with non-default permissions.
    destination = tmp_path / "profiles.csv"
    destination.write_text("old", encoding="utf-8")
    destination.chmod(0o640)

    # When a frame replaces that destination atomically.
    write_csv_atomically(pd.DataFrame({"ID": [1]}), destination)

    # Then the replacement retains the existing permission bits.
    assert stat.S_IMODE(destination.stat().st_mode) == 0o640


def test_atomic_csv_write_preserves_destination_when_serialization_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given an existing CSV and a serializer that writes partial data before failing.
    destination = tmp_path / "profiles.csv"
    destination.write_text("original", encoding="utf-8")

    def fail_after_partial_write(
        _frame: pd.DataFrame,
        path: str | Path,
        *,
        index: bool,
    ) -> None:
        assert not index
        Path(path).write_text("partial", encoding="utf-8")
        raise OSError("disk full")

    monkeypatch.setattr(pd.DataFrame, "to_csv", fail_after_partial_write)

    # When serialization fails during an atomic write.
    frame = pd.DataFrame({"ID": [1]})
    with pytest.raises(OSError, match="disk full"):
        write_csv_atomically(frame, destination)

    # Then the prior destination remains intact and partial files are removed.
    assert destination.read_text(encoding="utf-8") == "original"
    assert set(tmp_path.iterdir()) == {destination}


def test_atomic_csv_write_fsyncs_temporary_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fsync = Mock()
    monkeypatch.setattr(os, "fsync", fsync)

    write_csv_atomically(pd.DataFrame({"ID": [1]}), tmp_path / "profiles.csv")

    assert fsync.call_count >= 1


@pytest.mark.parametrize(
    "basename",
    [
        "checkpoint_new_" + "a" * 100 + ".csv.gz",
        "checkpoint_new_" + "a" * 64 + "." + "b" * 32 + ".csv",
    ],
)
def test_atomic_csv_temporary_path_does_not_repeat_long_basename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, basename: str
) -> None:
    destination = tmp_path / basename
    suffixes = "".join(destination.suffixes)
    temporary_paths: list[Path] = []
    replace = Path.replace

    def capture_replace(source: Path, target: str | Path) -> Path:
        temporary_paths.append(source)
        assert source.parent.parent == destination.parent
        assert source.parent.name.startswith(".csv-")
        assert source.name == "data" + suffixes
        assert len(str(source)) <= len(str(destination.parent)) + len(suffixes) + 25
        return replace(source, target)

    monkeypatch.setattr(Path, "replace", capture_replace)
    write_csv_atomically(pd.DataFrame({"ID": [1, 2]}), destination)
    assert len(temporary_paths) == 1
    assert pd.read_csv(destination)["ID"].tolist() == [1, 2]
    assert set(tmp_path.iterdir()) == {destination}


@pytest.mark.parametrize(
    ("basename", "member"),
    [
        ("profiles.csv.zip", "profiles.csv"),
        ("profiles.zip", "profiles"),
        ("profiles.csv.ZIP", "profiles.csv.ZIP"),
    ],
)
def test_atomic_csv_zip_member_name(tmp_path: Path, basename: str, member: str) -> None:
    destination = tmp_path / basename
    frame = pd.DataFrame({"ID": [1, 2]})

    write_csv_atomically(frame, destination)

    with ZipFile(destination) as archive:
        assert archive.namelist() == [member]
        with archive.open(member) as csv_file:
            pd.testing.assert_frame_equal(pd.read_csv(csv_file), frame)
    pd.testing.assert_frame_equal(pd.read_csv(destination), frame)
    assert set(tmp_path.iterdir()) == {destination}


@pytest.mark.parametrize(
    ("basename", "member"),
    [
        ("profiles.csv.tar", "profiles.csv"),
        ("profiles.tar", "profiles"),
        ("profiles.csv.tar.gz", "profiles.csv.tar.gz"),
        ("profiles.csv.tar.bz2", "profiles.csv.tar.bz2"),
        ("profiles.csv.tar.xz", "profiles.csv.tar.xz"),
        ("profiles.csv.TAR", "profiles.csv.TAR"),
    ],
)
def test_atomic_csv_tar_member_name(tmp_path: Path, basename: str, member: str) -> None:
    destination = tmp_path / basename
    frame = pd.DataFrame({"ID": [1, 2]})

    write_csv_atomically(frame, destination)

    with tarfile.open(destination) as archive:
        assert archive.getnames() == [member]
        csv_file = archive.extractfile(member)
        assert csv_file is not None
        with csv_file:
            pd.testing.assert_frame_equal(pd.read_csv(csv_file), frame)
    pd.testing.assert_frame_equal(pd.read_csv(destination), frame)
    assert set(tmp_path.iterdir()) == {destination}
