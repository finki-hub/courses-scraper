from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from concurrent.futures import (
    FIRST_COMPLETED,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass

from tqdm import tqdm

from app.checkpoints import (
    CheckpointPaths,
    CheckpointSnapshot,
)
from app.checkpoints import save as save_checkpoint
from app.coordinator_progress import ProgressState, abort_state, consume_future
from app.coordinator_state import RunState, build_snapshot, frame_records
from app.coordinator_workers import (
    WorkerDependencies,
    cancel_nonblocking,
    shutdown_waiting,
    submit,
)
from app.http import (
    InstanceHttpConfig,
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


def _initial_progress(plan: CoordinatorPlan) -> ProgressState:
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
            "new": frame_records(plan.initial.new.frame),
            "old": frame_records(plan.initial.old.frame),
        }
        requested_ids = plan.initial.requested_ids
    return ProgressState(
        requested_ids,
        profiles,
        completed,
        {},
        {},
        0.0,
    )


def _active_sides(
    plan: CoordinatorPlan,
) -> tuple[tuple[str, InstanceHttpConfig, Sequence[int]], ...]:
    return tuple(
        side
        for side in (
            ("new", plan.http_new, plan.profile_ids_new),
            ("old", plan.http_old, plan.profile_ids_old),
        )
        if side[2]
    )


def _preflight(
    sides: tuple[tuple[str, InstanceHttpConfig, Sequence[int]], ...],
) -> None:
    for _, config, _ in sides:
        with create_session(config) as session:
            preflight_instance(session, config)


def _submit_work(
    sides: tuple[tuple[str, InstanceHttpConfig, Sequence[int]], ...],
    state: ProgressState,
    dependencies: WorkerDependencies,
) -> None:
    for side, config, profile_ids in sides:
        state.work_by_side[side] = submit(config, profile_ids, dependencies)
    state.future_side.update(
        {
            future: side
            for side, work in state.work_by_side.items()
            for future in work.futures
        },
    )


def _save_progress(
    plan: CoordinatorPlan,
    state: ProgressState,
    now: float,
) -> None:
    save_checkpoint(
        plan.paths,
        build_snapshot(state.requested_ids, state.profiles, state.completed),
    )
    state.completed_since_save = 0
    state.dirty = False
    state.last_checkpoint_at = now


def _collect_pending(
    plan: CoordinatorPlan,
    state: ProgressState,
) -> CheckpointSnapshot:
    pending = set(state.future_side)
    with tqdm(total=len(pending)) as progress:
        while pending:
            done, pending = wait(
                pending,
                timeout=plan.poll_interval,
                return_when=FIRST_COMPLETED,
            )
            now = plan.monotonic_clock()
            for future in done:
                consume_future(future, state)
                progress.update()
                if state.dirty and state.completed_since_save >= plan.batch_size:
                    _save_progress(plan, state, now)
            if (
                state.dirty
                and now - state.last_checkpoint_at >= plan.checkpoint_interval
            ):
                _save_progress(plan, state, now)
    snapshot = build_snapshot(state.requested_ids, state.profiles, state.completed)
    save_checkpoint(plan.paths, snapshot)
    return snapshot


def _salvage_completed(state: ProgressState) -> None:
    for future in state.future_side:
        if future.done():
            try:
                consume_future(future, state)
            except Exception:
                logger.exception("Completed profile worker failed during interrupt")


def _ensure_complete(
    plan: CoordinatorPlan,
    snapshot: CheckpointSnapshot,
) -> None:
    remaining_new = frozenset(plan.profile_ids_new) - snapshot.new.completed_ids
    remaining_old = frozenset(plan.profile_ids_old) - snapshot.old.completed_ids
    if remaining_new or remaining_old:
        raise IncompleteScrapeError(remaining_new, remaining_old)


def run(plan: CoordinatorPlan) -> CheckpointSnapshot:
    state = _initial_progress(plan)
    sides = _active_sides(plan)
    _preflight(sides)
    dependencies = WorkerDependencies(ThreadPoolExecutor, create_session, fetch_profile)
    snapshot: CheckpointSnapshot
    try:
        _submit_work(sides, state, dependencies)
        state.last_checkpoint_at = plan.monotonic_clock()
        snapshot = _collect_pending(plan, state)
    except KeyboardInterrupt:
        _salvage_completed(state)
        return _abort(plan, abort_state(state), 130)
    except Exception:
        logger.exception("Active profile work failed; saving partial checkpoint")
        return _abort(plan, abort_state(state), 1)
    shutdown_waiting(state.work_by_side)
    _ensure_complete(plan, snapshot)
    return snapshot


def _abort(
    plan: CoordinatorPlan,
    state: RunState,
    exit_code: int,
) -> CheckpointSnapshot:
    snapshot = build_snapshot(state.requested_ids, state.profiles, state.completed)
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
