from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from app.constants import COL_ID, columns

_ID_PATTERN = re.compile(r"[1-9]\d*")
_LEGACY_COLUMNS = tuple(column for column in columns if column != "Timezone")


@dataclass(frozen=True, slots=True)
class CheckpointPaths:
    manifest: Path
    legacy_new: Path
    legacy_old: Path

    @classmethod
    def for_directory(cls, directory: Path) -> CheckpointPaths:
        return cls(
            manifest=directory / "checkpoint_manifest.json",
            legacy_new=directory / "checkpoint_new.csv",
            legacy_old=directory / "checkpoint_old.csv",
        )


@dataclass(frozen=True, slots=True)
class InstanceCheckpoint:
    frame: pd.DataFrame
    completed_ids: frozenset[int]


@dataclass(frozen=True, slots=True)
class CheckpointSnapshot:
    requested_ids: tuple[int, ...]
    new: InstanceCheckpoint
    old: InstanceCheckpoint


class CheckpointValidationError(Exception):
    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail

    def __str__(self) -> str:
        return f"invalid checkpoint: {self.detail}"


def fail_validation(detail: str) -> CheckpointValidationError:
    return CheckpointValidationError(detail=detail)


def request_fingerprint(requested_ids: tuple[int, ...]) -> str:
    payload = json.dumps(requested_ids, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def canonical_requested_ids(requested_ids: Sequence[int]) -> tuple[int, ...]:
    if any(profile_id <= 0 for profile_id in requested_ids):
        raise fail_validation("requested IDs must be positive")
    return tuple(sorted(set(requested_ids)))


def frame_ids(frame: pd.DataFrame, side: str) -> frozenset[int]:
    if list(frame.columns) != columns:
        raise fail_validation(f"{side} columns do not match the checkpoint schema")
    raw_ids = frame[COL_ID].astype(str).tolist()
    if any(_ID_PATTERN.fullmatch(raw_id) is None for raw_id in raw_ids):
        raise fail_validation(f"{side} IDs must be positive integers")
    profile_ids = [int(raw_id) for raw_id in raw_ids]
    if len(profile_ids) != len(set(profile_ids)):
        raise fail_validation(f"{side} IDs must be unique")
    return frozenset(profile_ids)


def normalize_legacy_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if tuple(frame.columns) == _LEGACY_COLUMNS:
        frame.insert(columns.index("Timezone"), "Timezone", "")
    return frame


def validate_snapshot(snapshot: CheckpointSnapshot) -> None:
    requested = snapshot.requested_ids
    if any(profile_id <= 0 for profile_id in requested) or len(requested) != len(
        set(requested),
    ):
        raise fail_validation("requested IDs must be positive and unique")
    requested_set = frozenset(requested)
    for side, checkpoint in (("new", snapshot.new), ("old", snapshot.old)):
        row_ids = frame_ids(checkpoint.frame, side)
        if not checkpoint.completed_ids <= requested_set:
            raise fail_validation(f"{side} completed IDs are outside the request")
        if not row_ids <= checkpoint.completed_ids:
            raise fail_validation(f"{side} rows must belong to completed IDs")
