from pathlib import Path

import pandas as pd
import pytest

from app.csv_io import write_csv_atomically


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
