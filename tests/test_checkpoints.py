from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pytest

from app import checkpoints
from app.checkpoints import (
    CheckpointPaths,
    CheckpointSnapshot,
    CheckpointValidationError,
    InstanceCheckpoint,
)
from app.constants import COL_ID, columns
from app.csv_io import write_bytes_atomically, write_csv_atomically

type Corruptor = Callable[[CheckpointPaths, dict[str, object]], None]


def _frame(*ids: int, skype: str = "") -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for profile_id in ids:
        row = dict.fromkeys(columns, "")
        row[COL_ID] = str(profile_id)
        row["Skype"] = skype
        rows.append(row)
    return pd.DataFrame(rows, columns=columns, dtype="string")


def _snapshot(
    requested: tuple[int, ...] = (1, 2, 3),
    *,
    new_rows: tuple[int, ...] = (1,),
    old_rows: tuple[int, ...] = (2,),
    completed_new: frozenset[int] = frozenset({1}),
    completed_old: frozenset[int] = frozenset({2}),
) -> CheckpointSnapshot:
    return CheckpointSnapshot(
        requested_ids=requested,
        new=InstanceCheckpoint(_frame(*new_rows), completed_new),
        old=InstanceCheckpoint(_frame(*old_rows), completed_old),
    )


def _manifest(paths: CheckpointPaths) -> dict[str, object]:
    value = json.loads(paths.manifest.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _assert_snapshot(
    actual: CheckpointSnapshot | None,
    expected: CheckpointSnapshot,
) -> None:
    assert actual is not None
    assert actual.requested_ids == expected.requested_ids
    assert actual.new.completed_ids == expected.new.completed_ids
    assert actual.old.completed_ids == expected.old.completed_ids
    pd.testing.assert_frame_equal(actual.new.frame, expected.new.frame)
    pd.testing.assert_frame_equal(actual.old.frame, expected.old.frame)


def test_save_commits_manifest_after_both_generation_csvs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = CheckpointPaths.for_directory(tmp_path)
    writes: list[str] = []
    real_csv_write = write_csv_atomically
    real_manifest_write = write_bytes_atomically

    def record_csv(frame: pd.DataFrame, destination: Path) -> None:
        writes.append(destination.name)
        real_csv_write(frame, destination)

    def record_manifest(destination: Path, payload: bytes) -> None:
        writes.append(destination.name)
        real_manifest_write(destination, payload)

    monkeypatch.setattr(checkpoints, "write_csv_atomically", record_csv)
    monkeypatch.setattr(checkpoints, "_write_manifest_atomically", record_manifest)

    checkpoints.save(paths, _snapshot())

    assert writes[-1] == paths.manifest.name
    assert writes[:2] == sorted(writes[:2])
    _assert_snapshot(checkpoints.load(paths, (1, 2, 3)), _snapshot())


def test_failed_generation_write_preserves_prior_manifest_selection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = CheckpointPaths.for_directory(tmp_path)
    original = _snapshot()
    checkpoints.save(paths, original)
    original_manifest = paths.manifest.read_bytes()
    real_csv_write = write_csv_atomically
    write_count = 0

    def fail_second_csv(frame: pd.DataFrame, destination: Path) -> None:
        nonlocal write_count
        write_count += 1
        if write_count == 2:
            raise OSError("injected write fault")
        real_csv_write(frame, destination)

    monkeypatch.setattr(checkpoints, "write_csv_atomically", fail_second_csv)

    with pytest.raises(OSError, match="injected write fault"):
        checkpoints.save(
            paths,
            _snapshot(
                new_rows=(1, 3),
                completed_new=frozenset({1, 3}),
            ),
        )

    assert paths.manifest.read_bytes() == original_manifest
    _assert_snapshot(checkpoints.load(paths, (1, 2, 3)), original)


def tmp_path_placeholder(
    paths: CheckpointPaths,
    manifest: dict[str, object],
    side: str,
) -> Path:
    instances = manifest["instances"]
    assert isinstance(instances, dict)
    instance = instances[side]
    assert isinstance(instance, dict)
    filename = instance["file"]
    assert isinstance(filename, str)
    return paths.manifest.parent / filename


def replace_manifest_hash(
    paths: CheckpointPaths,
    manifest: dict[str, object],
) -> None:
    instances = manifest["instances"]
    assert isinstance(instances, dict)
    new = instances["new"]
    assert isinstance(new, dict)
    new["sha256"] = "0" * 64
    paths.manifest.write_text(json.dumps(manifest), encoding="utf-8")


def replace_manifest_request(
    paths: CheckpointPaths,
    manifest: dict[str, object],
) -> None:
    request = manifest["request"]
    assert isinstance(request, dict)
    request["fingerprint"] = hashlib.sha256(b"other").hexdigest()
    paths.manifest.write_text(json.dumps(manifest), encoding="utf-8")


@pytest.mark.parametrize(
    "corrupt",
    [
        lambda paths, manifest: paths.manifest.write_text("{", encoding="utf-8"),
        lambda paths, manifest: paths.manifest.write_text("[]", encoding="utf-8"),
        lambda paths, manifest: Path(
            tmp_path_placeholder(paths, manifest, "new"),
        ).write_text("truncated", encoding="utf-8"),
        replace_manifest_hash,
        replace_manifest_request,
    ],
    ids=["malformed", "wrong-shape", "truncated-csv", "hash", "request"],
)
def test_load_fails_closed_for_corrupt_or_mismatched_checkpoint(
    tmp_path: Path,
    corrupt: Corruptor,
) -> None:
    paths = CheckpointPaths.for_directory(tmp_path)
    checkpoints.save(paths, _snapshot())
    manifest = _manifest(paths)
    corrupt(paths, manifest)

    with pytest.raises(CheckpointValidationError):
        checkpoints.load(paths, (1, 2, 3))


@pytest.mark.parametrize(
    ("frame", "completed", "requested"),
    [
        (pd.DataFrame({COL_ID: ["1"]}), frozenset({1}), (1,)),
        (_frame(0), frozenset({0}), (0,)),
        (_frame(1, 1), frozenset({1}), (1,)),
        (_frame(1), frozenset(), (1,)),
        (_frame(1), frozenset({1}), (2,)),
    ],
    ids=["columns", "positive", "unique", "row-completed", "request-membership"],
)
def test_save_rejects_invalid_checkpoint_membership(
    tmp_path: Path,
    frame: pd.DataFrame,
    completed: frozenset[int],
    requested: tuple[int, ...],
) -> None:
    snapshot = CheckpointSnapshot(
        requested_ids=requested,
        new=InstanceCheckpoint(frame, completed),
        old=InstanceCheckpoint(_frame(), frozenset()),
    )

    with pytest.raises(CheckpointValidationError):
        checkpoints.save(CheckpointPaths.for_directory(tmp_path), snapshot)


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
    assert loaded.completed_ids == frozenset({1})
    assert missing.frame.empty
    assert paths.manifest.exists()
    assert not paths.legacy_new.exists()
    assert not paths.legacy_old.exists()


def test_load_ignores_orphan_generation_files(tmp_path: Path) -> None:
    paths = CheckpointPaths.for_directory(tmp_path)
    checkpoints.save(paths, _snapshot())
    orphan = tmp_path / "checkpoint_new.orphan.csv"
    _frame(3).to_csv(orphan, index=False)

    loaded = checkpoints.load(paths, (1, 2, 3))

    _assert_snapshot(loaded, _snapshot())


def test_load_accepts_logically_identical_reordered_request_ids(tmp_path: Path) -> None:
    paths = CheckpointPaths.for_directory(tmp_path)
    checkpoints.save(paths, _snapshot(requested=(3, 1, 2)))

    loaded = checkpoints.load(paths, (2, 3, 1, 2))

    assert loaded is not None
    assert loaded.requested_ids == (1, 2, 3)


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


def test_successful_save_removes_only_superseded_strict_generations(
    tmp_path: Path,
) -> None:
    paths = CheckpointPaths.for_directory(tmp_path)
    checkpoints.save(paths, _snapshot())
    prior_generation_files = set(tmp_path.glob("checkpoint_*.csv"))
    unrelated = tmp_path / "checkpoint_new.keep.csv"
    unrelated.write_text("keep", encoding="utf-8")

    checkpoints.save(
        paths,
        _snapshot(new_rows=(1, 3), completed_new=frozenset({1, 3})),
    )

    manifest = _manifest(paths)
    generation = manifest["generation"]
    assert isinstance(generation, str)
    current = {
        tmp_path / f"checkpoint_new.{generation}.csv",
        tmp_path / f"checkpoint_old.{generation}.csv",
    }
    assert all(path.exists() for path in current)
    assert all(not path.exists() for path in prior_generation_files)
    assert unrelated.exists()


def test_manifest_write_failure_keeps_prior_committed_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = CheckpointPaths.for_directory(tmp_path)
    checkpoints.save(paths, _snapshot())
    prior_generation_files = set(tmp_path.glob("checkpoint_*.csv"))

    def fail_manifest(_destination: Path, _payload: bytes) -> None:
        raise OSError("manifest fault")

    monkeypatch.setattr(checkpoints, "_write_manifest_atomically", fail_manifest)

    with pytest.raises(OSError, match="manifest fault"):
        checkpoints.save(paths, _snapshot())

    assert all(path.exists() for path in prior_generation_files)


def test_clear_removes_manifest_selected_generation_and_legacy_files(
    tmp_path: Path,
) -> None:
    paths = CheckpointPaths.for_directory(tmp_path)
    checkpoints.save(paths, _snapshot())
    paths.legacy_new.write_text("legacy", encoding="utf-8")
    unrelated = tmp_path / "checkpoint_new.keep.csv"
    unrelated.write_text("keep", encoding="utf-8")

    checkpoints.clear(paths)

    assert list(tmp_path.iterdir()) == [unrelated]
