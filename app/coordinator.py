from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from concurrent.futures import (
    FIRST_COMPLETED,
    CancelledError,
    Future,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass
from typing import NoReturn, assert_never

import pandas as pd

from app.checkpoints import (
    CheckpointPaths,
    CheckpointSnapshot,
    InstanceCheckpoint,
)
from app.checkpoints import save as save_checkpoint
from app.constants import columns
from app.coordinator_workers import (
    InstanceWork,
    WorkerDependencies,
    cancel_nonblocking,
    shutdown_waiting,
    submit,
)
from app.http import (
    TRANSPORT_FAILURE_THRESHOLD,
    InstanceHttpConfig,
    ProfileEmpty,
    ProfileFetchOutcome,
    ProfileSuccess,
    ProfileTransportFailure,
    TransportFailureLimitError,
    create_session,
    fetch_profile,
    preflight_instance,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CoordinatorPlan:
    http_new: InstanceHttpConfig
    http_old: InstanceHttpConfig
    profile_ids_new: Sequence[int]
    profile_ids_old: Sequence[int]
    paths: CheckpointPaths
    initial: CheckpointSnapshot | None
    batch_size: int
    poll_interval: float
    checkpoint_interval: float
    monotonic_clock: Callable[[], float]
    terminate: Callable[[int], None]


class IncompleteScrapeError(Exception):
    def __init__(
        self,
        remaining_new: frozenset[int],
        remaining_old: frozenset[int],
    ) -> None:
        super().__init__(remaining_new, remaining_old)
        self.remaining_new = remaining_new
        self.remaining_old = remaining_old

    def __str__(self) -> str:
        return (
            "scrape incomplete: "
            f"new={sorted(self.remaining_new)}, old={sorted(self.remaining_old)}"
        )


@dataclass(frozen=True, slots=True)
class _RunState:
    requested_ids: tuple[int, ...]
    profiles: dict[str, list[dict[str, str]]]
    completed: dict[str, set[int]]
    work_by_side: dict[str, InstanceWork]
    future_side: dict[Future[ProfileFetchOutcome], str]


def _records(frame: pd.DataFrame) -> list[dict[str, str]]:
    return [
        {str(column): str(value) for column, value in row.items()}
        for row in frame.to_dict("records")
    ]


def _frame(profiles: list[dict[str, str]]) -> pd.DataFrame:
    frame = pd.DataFrame(profiles)
    for column in columns:
        if column not in frame.columns:
            frame[column] = ""
    return frame.loc[:, columns].copy()


def _snapshot(
    requested_ids: tuple[int, ...],
    profiles: dict[str, list[dict[str, str]]],
    completed: dict[str, set[int]],
) -> CheckpointSnapshot:
    return CheckpointSnapshot(
        requested_ids=requested_ids,
        new=InstanceCheckpoint(_frame(profiles["new"]), frozenset(completed["new"])),
        old=InstanceCheckpoint(_frame(profiles["old"]), frozenset(completed["old"])),
    )


def _raise_transport_failure(work: InstanceWork, failure_count: int) -> NoReturn:
    raise TransportFailureLimitError(
        base_url=work.config.base_url,
        failure_count=failure_count,
        threshold=TRANSPORT_FAILURE_THRESHOLD,
    )


def run(plan: CoordinatorPlan) -> CheckpointSnapshot:
    completed: dict[str, set[int]]
    profiles: dict[str, list[dict[str, str]]]
    if plan.initial is None:
        completed = {"new": set(), "old": set()}
        profiles = {"new": [], "old": []}
        requested_ids = tuple(
            sorted({*plan.profile_ids_new, *plan.profile_ids_old}),
        )
    else:
        completed = {
            "new": set(plan.initial.new.completed_ids),
            "old": set(plan.initial.old.completed_ids),
        }
        profiles = {
            "new": _records(plan.initial.new.frame),
            "old": _records(plan.initial.old.frame),
        }
        requested_ids = plan.initial.requested_ids
    work_by_side: dict[str, InstanceWork] = {}
    future_side: dict[Future[ProfileFetchOutcome], str] = {}
    sides = (
        ("new", plan.http_new, plan.profile_ids_new),
        ("old", plan.http_old, plan.profile_ids_old),
    )
    for _, config, profile_ids in sides:
        if not profile_ids:
            continue
        with create_session(config) as session:
            preflight_instance(session, config)
    dependencies = WorkerDependencies(ThreadPoolExecutor, create_session, fetch_profile)
    try:
        for side, config, profile_ids in sides:
            if not profile_ids:
                continue
            work_by_side[side] = submit(config, profile_ids, dependencies)
        future_side = {
            future: side
            for side, work in work_by_side.items()
            for future in work.futures
        }
        pending = set(future_side)
        handled: set[Future[ProfileFetchOutcome]] = set()
        transport_failures = {"new": 0, "old": 0}
        completed_since_save = 0
        dirty = False
        last_checkpoint_at = plan.monotonic_clock()

        def consume(future: Future[ProfileFetchOutcome]) -> None:
            nonlocal completed_since_save, dirty
            if future in handled:
                return
            handled.add(future)
            side = future_side[future]
            profile_id = work_by_side[side].futures[future]
            try:
                outcome = future.result()
            except CancelledError:
                return
            match outcome:
                case ProfileSuccess(profile=profile):
                    profiles[side].append(profile)
                    completed[side].add(profile_id)
                    completed_since_save += 1
                    dirty = True
                case ProfileEmpty():
                    completed[side].add(profile_id)
                    completed_since_save += 1
                    dirty = True
                case ProfileTransportFailure():
                    transport_failures[side] += 1
                    if transport_failures[side] >= TRANSPORT_FAILURE_THRESHOLD:
                        _raise_transport_failure(
                            work_by_side[side],
                            transport_failures[side],
                        )
                case unreachable:
                    assert_never(unreachable)

        while pending:
            done, pending = wait(
                pending,
                timeout=plan.poll_interval,
                return_when=FIRST_COMPLETED,
            )
            for future in done:
                consume(future)
            now = plan.monotonic_clock()
            if dirty and (
                completed_since_save >= plan.batch_size
                or now - last_checkpoint_at >= plan.checkpoint_interval
            ):
                save_checkpoint(
                    plan.paths,
                    _snapshot(requested_ids, profiles, completed),
                )
                completed_since_save = 0
                dirty = False
                last_checkpoint_at = now
        snapshot = _snapshot(requested_ids, profiles, completed)
        save_checkpoint(plan.paths, snapshot)
    except KeyboardInterrupt:
        for future in future_side:
            if future.done():
                try:
                    consume(future)
                except Exception:
                    logger.exception("Completed profile worker failed during interrupt")
        state = _RunState(
            requested_ids,
            profiles,
            completed,
            work_by_side,
            future_side,
        )
        return _abort(plan, state, 130)
    except Exception:
        logger.exception("Active profile work failed; saving partial checkpoint")
        state = _RunState(
            requested_ids,
            profiles,
            completed,
            work_by_side,
            future_side,
        )
        return _abort(plan, state, 1)
    shutdown_waiting(work_by_side)
    remaining_new = frozenset(plan.profile_ids_new) - snapshot.new.completed_ids
    remaining_old = frozenset(plan.profile_ids_old) - snapshot.old.completed_ids
    if remaining_new or remaining_old:
        raise IncompleteScrapeError(remaining_new, remaining_old)
    return snapshot


def _abort(
    plan: CoordinatorPlan,
    state: _RunState,
    exit_code: int,
) -> CheckpointSnapshot:
    snapshot = _snapshot(state.requested_ids, state.profiles, state.completed)
    try:
        cancel_nonblocking(state.work_by_side, state.future_side)
    except Exception:
        logger.exception("Nonblocking worker shutdown failed")
    try:
        save_checkpoint(plan.paths, snapshot)
    except Exception:
        logger.exception("Partial checkpoint save failed")
    finally:
        plan.terminate(exit_code)
    return snapshot
