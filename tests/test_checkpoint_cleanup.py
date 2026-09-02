from pathlib import Path

import pandas as pd
import pytest

from app import checkpoints
from app.checkpoints import CheckpointPaths, CheckpointSnapshot, InstanceCheckpoint
from app.constants import COL_ID, columns


def _snapshot(*completed_ids: int) -> CheckpointSnapshot:
    rows = [
        dict.fromkeys(columns, "") | {COL_ID: str(profile_id)}
        for profile_id in completed_ids
    ]
    frame = pd.DataFrame(rows, columns=columns, dtype="string")
    empty = pd.DataFrame(columns=columns, dtype="string")
    return CheckpointSnapshot(
        requested_ids=(1, 2),
        new=InstanceCheckpoint(frame, frozenset(completed_ids)),
        old=InstanceCheckpoint(empty, frozenset()),
    )


def test_post_commit_cleanup_failure_does_not_invalidate_save(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a prior checkpoint and a filesystem that rejects obsolete cleanup.
    paths = CheckpointPaths.for_directory(tmp_path)
    checkpoints.save(paths, _snapshot(1))

    def reject_cleanup(_directory: Path, _generation: str) -> None:
        raise PermissionError

    monkeypatch.setattr(checkpoints, "remove_superseded", reject_cleanup)

    # When a newer checkpoint commits, then cleanup cannot invalidate that save.
    checkpoints.save(paths, _snapshot(1, 2))

    loaded = checkpoints.load(paths, (1, 2))
    assert loaded is not None
    assert loaded.new.completed_ids == frozenset({1, 2})
