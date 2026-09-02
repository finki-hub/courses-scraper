from pathlib import Path

import pandas as pd
import pytest

from app import export
from app.export import EmptyExportError, export_profiles


def test_export_neutralizes_formula_strings_on_a_deep_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given formula-like strings, ordinary strings, and non-string values.
    frame = pd.DataFrame(
        {
            "plain": ["safe", "  =SUM(A1:A2)", "\t+cmd", " -1", "@user"],
            "number": [1, 2, 3, 4, 5],
        },
    )
    original = frame.copy(deep=True)
    written: list[pd.DataFrame] = []

    def capture(candidate: pd.DataFrame, _destination: Path) -> None:
        written.append(candidate)

    monkeypatch.setattr(export, "write_csv_atomically", capture)

    # When the frame crosses the final export boundary.
    export_profiles(frame, tmp_path / "profiles.csv")

    # Then only dangerous strings in the exported copy are apostrophe-prefixed.
    assert written[0]["plain"].tolist() == [
        "safe",
        "'  =SUM(A1:A2)",
        "'\t+cmd",
        "' -1",
        "'@user",
    ]
    assert written[0]["number"].tolist() == [1, 2, 3, 4, 5]
    pd.testing.assert_frame_equal(frame, original)
    assert written[0] is not frame


def test_empty_export_preserves_existing_destination(tmp_path: Path) -> None:
    # Given an existing final CSV and an empty candidate frame.
    destination = tmp_path / "profiles.csv"
    destination.write_text("prior output", encoding="utf-8")

    # When final export is attempted.
    with pytest.raises(EmptyExportError, match="empty"):
        export_profiles(pd.DataFrame(columns=["ID"]), destination)

    # Then the prior output is untouched.
    assert destination.read_text(encoding="utf-8") == "prior output"
