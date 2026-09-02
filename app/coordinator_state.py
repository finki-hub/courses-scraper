from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass

import pandas as pd

from app.checkpoints import CheckpointSnapshot, InstanceCheckpoint
from app.constants import columns
from app.coordinator_workers import InstanceWork
from app.http import ProfileFetchOutcome


@dataclass(frozen=True, slots=True)
class RunState:
    requested_ids: tuple[int, ...]
    profiles: dict[str, list[dict[str, str]]]
    completed: dict[str, set[int]]
    work_by_side: dict[str, InstanceWork]
    future_side: dict[Future[ProfileFetchOutcome], str]


def frame_records(frame: pd.DataFrame) -> list[dict[str, str]]:
    return [
        {str(column): str(value) for column, value in row.items()}
        for row in frame.to_dict("records")
    ]


def build_snapshot(
    requested_ids: tuple[int, ...],
    profiles: dict[str, list[dict[str, str]]],
    completed: dict[str, set[int]],
) -> CheckpointSnapshot:
    frames: dict[str, pd.DataFrame] = {}
    for side in ("new", "old"):
        frame = pd.DataFrame(profiles[side])
        for column in columns:
            if column not in frame.columns:
                frame[column] = ""
        frames[side] = frame.loc[:, columns].copy()
    return CheckpointSnapshot(
        requested_ids=requested_ids,
        new=InstanceCheckpoint(frames["new"], frozenset(completed["new"])),
        old=InstanceCheckpoint(frames["old"], frozenset(completed["old"])),
    )
