import hashlib
from pathlib import Path

import pandas as pd
import pytest

from app.constants import columns
from app.export import export_profiles
from app.historical import (
    HistoricalDataError,
    load_historical_snapshot,
    merge_historical_snapshot,
)
from app.profile_merge import MERGED_COLUMNS


def _row(old: str = "", new: str = "", **values: str) -> dict[str, str]:
    return dict.fromkeys(MERGED_COLUMNS, "") | {
        "ID_old": old,
        "ID_new": new,
        **values,
    }


def _write(path: Path, rows: list[dict[str, str]]) -> Path:
    pd.DataFrame(rows, columns=list(MERGED_COLUMNS)).to_csv(path, index=False)
    return path


def test_refresh_preserves_history_and_is_repeatable(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "history.csv",
        [
            _row(
                "2",
                "2",
                Name="Prior",
                Mail="NA",
                Courses="Old\nShared",
                Description_old="old",
                Description_new="stale",
                Profile_old="old-url",
            ),
            _row("3", Name="Old only"),
            _row(new="20000", Name="Unmatched"),
            _row("4", Name="Unmatched old"),
        ],
    )
    snapshot = load_historical_snapshot(path)
    before = snapshot.frame.copy(deep=True)
    raw_bytes = path.read_bytes()
    fresh = (
        pd.DataFrame(
            [
                {"ID": "2", "Name": "Current", "Courses": "Fresh\nShared"},
                {"ID": "3", "Name": "Joined"},
                {"ID": "10", "Name": "Appended"},
            ],
            columns=columns,
        )
        .astype("string")
        .fillna("")
    )
    result = merge_historical_snapshot(snapshot, fresh)
    assert result["ID_new"].tolist() == ["2", "3", "10", "20000", ""]
    row = result.iloc[0]
    assert row["Description_old"] == "old"
    assert row["Profile_old"] == "old-url"
    assert row["Description_new"] == ""
    assert row["Name"] == "Current"
    assert row["Mail"] == "NA"
    assert row["Courses"] == "Fresh\nShared\nOld"
    assert row["Courses Count"] == "3"
    assert result.iloc[1]["ID_old"] == "3"
    pd.testing.assert_frame_equal(snapshot.frame, before)
    assert path.read_bytes() == raw_bytes
    assert snapshot.sha256 == hashlib.sha256(raw_bytes).hexdigest()
    assert snapshot.path == path.resolve()
    exported = tmp_path / "refreshed.csv"
    export_profiles(result, exported)
    repeated = merge_historical_snapshot(load_historical_snapshot(exported), fresh)
    pd.testing.assert_frame_equal(result, repeated)


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [_row()],
        [_row("16339")],
        [_row("2", "3")],
        [_row(new="1.0")],
        [_row(new="0")],
        [_row(new="-1")],
        [_row(new="\u0661")],
        [_row(new="1"), _row(new="01")],
        [_row("2"), _row("2")],
    ],
)
def test_invalid_rows_fail_safely(
    tmp_path: Path,
    rows: list[dict[str, str]],
) -> None:
    path = _write(tmp_path / "invalid.csv", rows)
    with pytest.raises(HistoricalDataError, match="historical snapshot"):
        load_historical_snapshot(path)


@pytest.mark.parametrize(
    "header",
    [
        "ID_old,ID_old",
        ",".join(reversed(MERGED_COLUMNS)),
        "ID,Name",
    ],
)
def test_invalid_schema(tmp_path: Path, header: str) -> None:
    path = tmp_path / "invalid.csv"
    path.write_text(header + "\n", encoding="utf-8")
    with pytest.raises(HistoricalDataError):
        load_historical_snapshot(path)


def test_export_does_not_double_escape_history(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "history.csv",
        [
            _row(new="1", Name="'=safe", Description_new="'@safe"),
        ],
    )
    result = merge_historical_snapshot(
        load_historical_snapshot(path),
        pd.DataFrame(columns=columns),
    )
    export_profiles(result, tmp_path / "out.csv")
    reloaded = load_historical_snapshot(tmp_path / "out.csv").frame
    assert reloaded.iloc[0]["Name"] == "'=safe"
    assert reloaded.iloc[0]["Description_new"] == "'@safe"


@pytest.mark.parametrize(
    "content",
    [
        b"",
        b"\xff",
        b"ID_old,ID_new\n1,2,3\n",
        (",".join(MERGED_COLUMNS) + '\n"unfinished').encode(),
    ],
)
def test_unreadable_csv_is_public_error(tmp_path: Path, content: bytes) -> None:
    path = tmp_path / "invalid.csv"
    path.write_bytes(content)
    with pytest.raises(HistoricalDataError):
        load_historical_snapshot(path)


@pytest.mark.parametrize(("old", "new"), [("2", "2"), ("002", "02")])
def test_split_migrated_identity_is_rejected(
    tmp_path: Path,
    old: str,
    new: str,
) -> None:
    path = _write(tmp_path / "split.csv", [_row(old), _row(new=new)])
    with pytest.raises(HistoricalDataError):
        load_historical_snapshot(path)


def test_missing_snapshot_is_public_error(tmp_path: Path) -> None:
    with pytest.raises(HistoricalDataError):
        load_historical_snapshot(tmp_path / "missing.csv")


def test_numeric_sort_and_canonical_ids(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "history.csv",
        [
            _row(new="010"),
            _row(new="02"),
            _row("09"),
            _row("1"),
        ],
    )
    result = merge_historical_snapshot(
        load_historical_snapshot(path),
        pd.DataFrame(columns=columns),
    )
    assert result["ID_new"].tolist() == ["2", "10", "", ""]
    assert result["ID_old"].tolist() == ["", "", "1", "9"]
