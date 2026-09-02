from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from threading import Lock, local

import requests

from app.http import InstanceHttpConfig, ProfileFetchOutcome


@dataclass(frozen=True, slots=True)
class WorkerDependencies:
    executor_factory: Callable[[int], ThreadPoolExecutor]
    create_session: Callable[[InstanceHttpConfig], requests.Session]
    fetch_profile: Callable[
        [requests.Session, int, InstanceHttpConfig],
        ProfileFetchOutcome,
    ]


class _WorkerSession(local):
    session: requests.Session | None

    def __init__(self) -> None:
        self.session = None


@dataclass(frozen=True, slots=True)
class InstanceWork:
    config: InstanceHttpConfig
    executor: ThreadPoolExecutor
    futures: dict[Future[ProfileFetchOutcome], int]
    sessions: list[requests.Session]


def submit(
    config: InstanceHttpConfig,
    profile_ids: Sequence[int],
    dependencies: WorkerDependencies,
) -> InstanceWork:
    executor = dependencies.executor_factory(config.threads)
    worker_state = _WorkerSession()
    sessions: list[requests.Session] = []
    sessions_lock = Lock()

    def fetch(profile_id: int) -> ProfileFetchOutcome:
        if worker_state.session is None:
            worker_state.session = dependencies.create_session(config)
            with sessions_lock:
                sessions.append(worker_state.session)
        return dependencies.fetch_profile(worker_state.session, profile_id, config)

    try:
        futures = {
            executor.submit(fetch, profile_id): profile_id for profile_id in profile_ids
        }
    except RuntimeError:
        executor.shutdown(wait=True, cancel_futures=True)
        for session in sessions:
            session.close()
        raise
    return InstanceWork(config, executor, futures, sessions)


def shutdown_waiting(work_by_side: dict[str, InstanceWork]) -> None:
    for work in work_by_side.values():
        work.executor.shutdown(wait=True)
        for session in work.sessions:
            session.close()


def cancel_nonblocking(
    work_by_side: dict[str, InstanceWork],
    futures: dict[Future[ProfileFetchOutcome], str],
) -> None:
    for future in futures:
        future.cancel()
    for work in work_by_side.values():
        work.executor.shutdown(wait=False, cancel_futures=True)
