import os
import stat
from pathlib import Path

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
    with pytest.raises(OSError, match="disk full"):
        write_csv_atomically(pd.DataFrame({"ID": [1]}), destination)

    # Then the prior destination remains intact and partial files are removed.
    assert destination.read_text(encoding="utf-8") == "original"
    assert set(tmp_path.iterdir()) == {destination}
