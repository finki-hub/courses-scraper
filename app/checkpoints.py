from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Final
from uuid import uuid4

import pandas as pd

from app.checkpoint_generations import remove_all, remove_superseded
from app.checkpoint_schema import (
    CheckpointPaths,
    CheckpointSnapshot,
    CheckpointValidationError,
    InstanceCheckpoint,
    canonical_requested_ids,
    fail_validation,
    frame_ids,
    validate_snapshot,
)
from app.constants import COL_ID, columns
from app.csv_io import write_bytes_atomically as _write_manifest_atomically
from app.csv_io import write_csv_atomically

__all__ = (
    "CheckpointPaths",
    "CheckpointSnapshot",
    "CheckpointValidationError",
    "InstanceCheckpoint",
    "clear",
    "load",
    "save",
)

_MANIFEST_VERSION: Final = 1


def _request_fingerprint(requested_ids: tuple[int, ...]) -> str:
    payload = json.dumps(requested_ids, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def save(paths: CheckpointPaths, snapshot: CheckpointSnapshot) -> None:
    snapshot = CheckpointSnapshot(
        requested_ids=canonical_requested_ids(snapshot.requested_ids),
        new=snapshot.new,
        old=snapshot.old,
    )
    validate_snapshot(snapshot)
    generation = uuid4().hex
    directory = paths.manifest.parent
    directory.mkdir(parents=True, exist_ok=True)
    filenames = {
        "new": f"checkpoint_new.{generation}.csv",
        "old": f"checkpoint_old.{generation}.csv",
    }
    checkpoint_by_side = {"new": snapshot.new, "old": snapshot.old}
    for side in ("new", "old"):
        write_csv_atomically(
            checkpoint_by_side[side].frame,
            directory / filenames[side],
        )
    instances = {
        side: {
            "file": filenames[side],
            "rows": len(checkpoint_by_side[side].frame),
            "completed_ids": sorted(checkpoint_by_side[side].completed_ids),
            "sha256": _sha256(directory / filenames[side]),
        }
        for side in ("new", "old")
    }
    manifest = {
        "version": _MANIFEST_VERSION,
        "generation": generation,
        "columns": columns,
        "request": {
            "ids": list(snapshot.requested_ids),
            "count": len(snapshot.requested_ids),
            "fingerprint": _request_fingerprint(snapshot.requested_ids),
        },
        "instances": instances,
    }
    payload = (
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    _write_manifest_atomically(paths.manifest, payload)
    remove_superseded(directory, generation)


def _mapping(value: object, detail: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise fail_validation(detail)
    return value


def _integer_list(value: object, detail: str) -> tuple[int, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, int) or isinstance(item, bool) for item in value
    ):
        raise fail_validation(detail)
    return tuple(value)


def _load_instance(
    directory: Path,
    side: str,
    raw: object,
    requested: frozenset[int],
    generation: str,
) -> InstanceCheckpoint:
    instance = _mapping(raw, f"{side} manifest entry is malformed")
    filename = instance.get("file")
    rows = instance.get("rows")
    sha256 = instance.get("sha256")
    if (
        not isinstance(filename, str)
        or filename != f"checkpoint_{side}.{generation}.csv"
        or not isinstance(rows, int)
        or isinstance(rows, bool)
        or rows < 0
        or not isinstance(sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
    ):
        raise fail_validation(f"{side} manifest metadata is malformed")
    completed_values = _integer_list(
        instance.get("completed_ids"),
        f"{side} completed IDs malformed",
    )
    if any(profile_id <= 0 for profile_id in completed_values) or len(
        completed_values,
    ) != len(set(completed_values)):
        raise fail_validation(f"{side} completed IDs must be positive and unique")
    completed_ids = frozenset(completed_values)
    path = directory / filename
    if not path.is_file() or _sha256(path) != sha256:
        raise fail_validation(f"{side} checkpoint content does not match its manifest")
    try:
        frame = pd.read_csv(path, dtype="string", keep_default_na=False)
    except (OSError, pd.errors.ParserError, pd.errors.EmptyDataError) as error:
        raise fail_validation(f"{side} checkpoint CSV cannot be read") from error
    if len(frame) != rows:
        raise fail_validation(
            f"{side} checkpoint row count does not match its manifest",
        )
    row_ids = frame_ids(frame, side)
    if not completed_ids <= requested or not row_ids <= completed_ids:
        raise fail_validation(f"{side} checkpoint membership is invalid")
    return InstanceCheckpoint(frame=frame, completed_ids=completed_ids)


def _load_manifest(
    paths: CheckpointPaths,
    requested_ids: tuple[int, ...],
) -> CheckpointSnapshot:
    try:
        raw = json.loads(paths.manifest.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise fail_validation("manifest cannot be read") from error
    manifest = _mapping(raw, "manifest root is malformed")
    if (
        manifest.get("version") != _MANIFEST_VERSION
        or manifest.get("columns") != columns
    ):
        raise fail_validation("manifest version or columns do not match")
    generation = manifest.get("generation")
    if (
        not isinstance(generation, str)
        or re.fullmatch(
            r"[0-9a-f]{32}",
            generation,
        )
        is None
    ):
        raise fail_validation("manifest generation is malformed")
    request = _mapping(manifest.get("request"), "request metadata is malformed")
    stored_ids = _integer_list(request.get("ids"), "request IDs are malformed")
    if (
        request.get("count") != len(stored_ids)
        or request.get("fingerprint") != _request_fingerprint(stored_ids)
        or stored_ids != requested_ids
    ):
        raise fail_validation("checkpoint request does not match the current request")
    instances = _mapping(manifest.get("instances"), "instances metadata is malformed")
    requested = frozenset(requested_ids)
    snapshot = CheckpointSnapshot(
        requested_ids=requested_ids,
        new=_load_instance(
            paths.manifest.parent,
            "new",
            instances.get("new"),
            requested,
            generation,
        ),
        old=_load_instance(
            paths.manifest.parent,
            "old",
            instances.get("old"),
            requested,
            generation,
        ),
    )
    validate_snapshot(snapshot)
    return snapshot


def _load_legacy(
    path: Path,
    side: str,
    requested: frozenset[int],
) -> InstanceCheckpoint:
    if not path.exists():
        return InstanceCheckpoint(
            frame=pd.DataFrame(columns=columns, dtype="string"),
            completed_ids=frozenset(),
        )
    try:
        frame = pd.read_csv(path, dtype="string", keep_default_na=False)
    except (OSError, pd.errors.ParserError, pd.errors.EmptyDataError) as error:
        raise fail_validation(f"legacy {side} checkpoint cannot be read") from error
    profile_ids = frame_ids(frame, side)
    included = frame[COL_ID].astype(str).map(int).isin(requested)
    filtered = frame.loc[included].reset_index(drop=True)
    return InstanceCheckpoint(
        frame=filtered,
        completed_ids=profile_ids & requested,
    )


def load(
    paths: CheckpointPaths,
    requested_ids: tuple[int, ...] | list[int] | range,
) -> CheckpointSnapshot | None:
    requested = canonical_requested_ids(tuple(requested_ids))
    if paths.manifest.exists():
        return _load_manifest(paths, requested)
    if not paths.legacy_new.exists() and not paths.legacy_old.exists():
        return None
    snapshot = CheckpointSnapshot(
        requested_ids=requested,
        new=_load_legacy(paths.legacy_new, "new", frozenset(requested)),
        old=_load_legacy(paths.legacy_old, "old", frozenset(requested)),
    )
    validate_snapshot(snapshot)
    save(paths, snapshot)
    paths.legacy_new.unlink(missing_ok=True)
    paths.legacy_old.unlink(missing_ok=True)
    return snapshot


def clear(paths: CheckpointPaths) -> None:
    paths.manifest.unlink(missing_ok=True)
    paths.legacy_new.unlink(missing_ok=True)
    paths.legacy_old.unlink(missing_ok=True)
    remove_all(paths.manifest.parent)
