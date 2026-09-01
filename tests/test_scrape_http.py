from threading import Barrier, Lock, get_ident

import pytest
import requests

import app.__main__ as scraper
from app.constants import selectors_new
from app.http import (
    TRANSPORT_FAILURE_THRESHOLD,
    EmptyReason,
    InstanceHttpConfig,
    ProfileEmpty,
    ProfileTransportFailure,
    TransportFailureLimitError,
)

BASE_URL = "https://courses.finki.ukim.mk"


def _config(*, threads: int = 1) -> InstanceHttpConfig:
    return InstanceHttpConfig(
        base_url=BASE_URL,
        cookie="secret-cookie",
        selectors=selectors_new,
        threads=threads,
    )


def test_profile_workers_use_one_session_per_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given two concurrent workers and a session-recording profile fetch.
    barrier = Barrier(2)
    lock = Lock()
    sessions_by_thread: dict[int, set[int]] = {}
    created_sessions: list[requests.Session] = []

    def create_worker_session(_config: InstanceHttpConfig) -> requests.Session:
        session = requests.Session()
        created_sessions.append(session)
        return session

    def record_fetch(
        session: requests.Session,
        profile_id: int,
        _config: InstanceHttpConfig,
    ) -> ProfileEmpty:
        with lock:
            sessions_by_thread.setdefault(get_ident(), set()).add(id(session))
        barrier.wait(timeout=2)
        return ProfileEmpty(
            profile_id=profile_id,
            reason=EmptyReason.EMPTY_PROFILE,
            status_code=200,
        )

    monkeypatch.setattr(scraper, "create_session", create_worker_session)
    monkeypatch.setattr(scraper, "fetch_profile", record_fetch)

    # When both profile requests run concurrently.
    scraper.get_profiles(_config(threads=2), [1, 2])

    # Then each worker owns exactly one distinct session.
    assert len(created_sessions) == 2
    assert len(sessions_by_thread) == 2
    assert all(len(session_ids) == 1 for session_ids in sessions_by_thread.values())
    assert len(set.union(*sessions_by_thread.values())) == 2


def test_transport_failure_threshold_aborts_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given requests that all fail at the transport boundary.
    monkeypatch.setattr(scraper, "create_session", lambda _config: requests.Session())
    monkeypatch.setattr(
        scraper,
        "fetch_profile",
        lambda _session, profile_id, _config: ProfileTransportFailure(
            profile_id=profile_id,
        ),
    )

    # When the documented threshold is reached, then scraping aborts.
    with pytest.raises(TransportFailureLimitError) as caught:
        scraper.get_profiles(
            _config(),
            list(range(1, TRANSPORT_FAILURE_THRESHOLD + 1)),
        )

    assert caught.value.failure_count == TRANSPORT_FAILURE_THRESHOLD
    assert caught.value.threshold == TRANSPORT_FAILURE_THRESHOLD


def test_all_transport_failures_abort_small_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a batch smaller than the fixed threshold with no HTTP response at all.
    monkeypatch.setattr(scraper, "create_session", lambda _config: requests.Session())
    monkeypatch.setattr(
        scraper,
        "fetch_profile",
        lambda _session, profile_id, _config: ProfileTransportFailure(
            profile_id=profile_id,
        ),
    )

    # When the batch finishes, then an outage cannot be reported as empty success.
    with pytest.raises(TransportFailureLimitError) as caught:
        scraper.get_profiles(_config(), [1, 2])

    assert caught.value.failure_count == 2
    assert caught.value.threshold == TRANSPORT_FAILURE_THRESHOLD
