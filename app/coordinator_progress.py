from __future__ import annotations

from concurrent.futures import CancelledError, Future
from dataclasses import dataclass, field
from typing import NoReturn, assert_never

from app.coordinator_state import RunState
from app.coordinator_workers import InstanceWork
from app.http import (
    ProfileEmpty,
    ProfileFetchOutcome,
    ProfileSuccess,
    ProfileTransportFailure,
    TransportFailureLimitError,
    transport_failure_rate_exceeded,
)


@dataclass(slots=True)
class ProgressState:
    requested_ids: tuple[int, ...]
    profiles: dict[str, list[dict[str, str]]]
    completed: dict[str, set[int]]
    work_by_side: dict[str, InstanceWork]
    future_side: dict[Future[ProfileFetchOutcome], str]
    last_checkpoint_at: float
    handled: set[Future[ProfileFetchOutcome]] = field(default_factory=set)
    transport_failures: dict[str, int] = field(
        default_factory=lambda: {"new": 0, "old": 0},
    )
    outcomes_seen: dict[str, int] = field(
        default_factory=lambda: {"new": 0, "old": 0},
    )
    completed_since_save: int = 0
    dirty: bool = False


def _raise_transport_failure(
    work: InstanceWork,
    failure_count: int,
    outcome_count: int,
) -> NoReturn:
    raise TransportFailureLimitError(
        base_url=work.config.base_url,
        failure_count=failure_count,
        outcome_count=outcome_count,
    )


def consume_future(
    future: Future[ProfileFetchOutcome],
    state: ProgressState,
) -> None:
    if future in state.handled:
        return
    state.handled.add(future)
    side = state.future_side[future]
    work = state.work_by_side[side]
    profile_id = work.futures[future]
    try:
        outcome = future.result()
    except CancelledError:
        return
    state.outcomes_seen[side] += 1
    match outcome:
        case ProfileSuccess(profile=profile):
            state.profiles[side].append(profile)
            state.completed[side].add(profile_id)
            state.completed_since_save += 1
            state.dirty = True
        case ProfileEmpty():
            state.completed[side].add(profile_id)
            state.completed_since_save += 1
            state.dirty = True
        case ProfileTransportFailure():
            state.transport_failures[side] += 1
        case unreachable:
            assert_never(unreachable)
    if transport_failure_rate_exceeded(
        state.transport_failures[side],
        state.outcomes_seen[side],
        len(work.futures),
    ):
        _raise_transport_failure(
            work,
            state.transport_failures[side],
            state.outcomes_seen[side],
        )


def abort_state(state: ProgressState) -> RunState:
    return RunState(
        state.requested_ids,
        state.profiles,
        state.completed,
        state.work_by_side,
        state.future_side,
    )
