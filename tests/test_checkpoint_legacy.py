from pathlib import Path

import pandas as pd
import pytest

from app import checkpoints
from app.checkpoints import CheckpointPaths
from app.constants import COL_ID, columns

LEGACY_COLUMNS = [column for column in columns if column != "Timezone"]


def _frame(*ids: int, skype: str = "") -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for profile_id in ids:
        row = dict.fromkeys(LEGACY_COLUMNS, "")
        row[COL_ID] = str(profile_id)
        row["Skype"] = skype
        rows.append(row)
    return pd.DataFrame(rows, columns=LEGACY_COLUMNS, dtype="string")


@pytest.mark.parametrize("legacy_side", ["new", "old"])
def test_load_migrates_one_sided_legacy_checkpoint_preserving_text(
    tmp_path: Path,
    legacy_side: str,
) -> None:
    paths = CheckpointPaths.for_directory(tmp_path)
    legacy = paths.legacy_new if legacy_side == "new" else paths.legacy_old
    _frame(1, skype="00123").to_csv(legacy, index=False)

    snapshot = checkpoints.load(paths, (1, 2))

    assert snapshot is not None
    loaded = snapshot.new if legacy_side == "new" else snapshot.old
    missing = snapshot.old if legacy_side == "new" else snapshot.new
    assert loaded.frame.loc[0, "Skype"] == "00123"
    assert loaded.frame.loc[0, "Timezone"] == ""
    assert loaded.frame.columns.tolist() == columns
    assert loaded.completed_ids == frozenset({1})
    assert missing.frame.empty
    assert paths.manifest.exists()
    assert not paths.legacy_new.exists()
    assert not paths.legacy_old.exists()


def test_load_does_not_fail_after_committing_when_legacy_cleanup_is_denied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = CheckpointPaths.for_directory(tmp_path)
    _frame(1).to_csv(paths.legacy_new, index=False)
    real_unlink = Path.unlink

    def reject_legacy_cleanup(path: Path, *, missing_ok: bool = False) -> None:
        if path == paths.legacy_new:
            raise PermissionError
        real_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", reject_legacy_cleanup)

    snapshot = checkpoints.load(paths, (1,))

    assert snapshot is not None
    assert snapshot.new.completed_ids == frozenset({1})
    assert paths.manifest.exists()
    assert paths.legacy_new.exists()


def test_legacy_migration_filters_rows_outside_current_request_preserving_text(
    tmp_path: Path,
) -> None:
    paths = CheckpointPaths.for_directory(tmp_path)
    legacy = pd.concat(
        [_frame(1, skype="00123"), _frame(4, skype="NA")],
        ignore_index=True,
    )
    legacy.to_csv(paths.legacy_new, index=False)

    loaded = checkpoints.load(paths, (1, 2))

    assert loaded is not None
    assert loaded.new.frame[COL_ID].tolist() == ["1"]
    assert loaded.new.frame.loc[0, "Skype"] == "00123"
    assert loaded.new.completed_ids == frozenset({1})
