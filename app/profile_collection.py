from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from threading import Lock, local
from typing import assert_never

import requests
from tqdm import tqdm

from app.http import (
    InstanceHttpConfig,
    ProfileEmpty,
    ProfileFetchOutcome,
    ProfileSuccess,
    ProfileTransportFailure,
    TransportFailureLimitError,
    transport_failure_rate_exceeded,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ProfileCollectionDependencies:
    create_session: Callable[[InstanceHttpConfig], requests.Session]
    fetch_profile: Callable[
        [requests.Session, int, InstanceHttpConfig],
        ProfileFetchOutcome,
    ]


class _WorkerSession(local):
    session: requests.Session | None

    def __init__(self) -> None:
        self.session = None


def collect_profiles(
    config: InstanceHttpConfig,
    profile_ids: Sequence[int],
    dependencies: ProfileCollectionDependencies,
) -> list[dict[str, str]]:
    profiles: list[dict[str, str]] = []
    requested_ids = list(profile_ids)
    transport_failures = 0
    outcomes_seen = 0
    worker_state = _WorkerSession()
    worker_sessions: list[requests.Session] = []
    worker_sessions_lock = Lock()
    executor = ThreadPoolExecutor(max_workers=config.threads)

    def fetch(profile_id: int) -> ProfileFetchOutcome:
        if worker_state.session is None:
            worker_state.session = dependencies.create_session(config)
            with worker_sessions_lock:
                worker_sessions.append(worker_state.session)
        return dependencies.fetch_profile(worker_state.session, profile_id, config)

    futures: dict[Future[ProfileFetchOutcome], int] = {}
    aborted = False
    try:
        futures = {
            executor.submit(fetch, profile_id): profile_id
            for profile_id in requested_ids
        }
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc=config.base_url.rsplit("//", maxsplit=1)[-1],
        ):
            try:
                outcome = future.result()
            except CancelledError:
                continue
            outcomes_seen += 1
            match outcome:
                case ProfileSuccess(profile=profile):
                    profiles.append(profile)
                case ProfileEmpty():
                    pass
                case ProfileTransportFailure(profile_id=profile_id):
                    transport_failures += 1
                    logger.warning(
                        "Transport failed for profile %d from %s",
                        profile_id,
                        config.base_url,
                    )
                case unreachable:
                    assert_never(unreachable)
            if transport_failure_rate_exceeded(
                transport_failures,
                outcomes_seen,
                len(requested_ids),
            ):
                aborted = True
                for pending in futures:
                    pending.cancel()
                executor.shutdown(wait=False, cancel_futures=True)
                raise TransportFailureLimitError(
                    base_url=config.base_url,
                    failure_count=transport_failures,
                    outcome_count=outcomes_seen,
                )
    except KeyboardInterrupt:
        aborted = True
        for pending in futures:
            pending.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    finally:
        if not aborted:
            executor.shutdown(wait=True)
        for session in worker_sessions:
            session.close()
    if transport_failures == len(requested_ids) and transport_failures > 0:
        raise TransportFailureLimitError(
            base_url=config.base_url,
            failure_count=transport_failures,
            outcome_count=len(requested_ids),
        )
    return profiles
