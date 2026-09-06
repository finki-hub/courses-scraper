from collections.abc import Callable
from concurrent.futures import Future
from threading import Barrier, Lock, get_ident
from unittest.mock import Mock

import pytest
import requests

import app.__main__ as scraper
from app import profile_collection
from app.auth import InstanceCookies
from app.constants import selectors_new
from app.http import (
    TRANSPORT_FAILURE_THRESHOLD,
    EmptyReason,
    InstanceHttpConfig,
    ProfileEmpty,
    ProfileSuccess,
    ProfileTransportFailure,
    TransportFailureLimitError,
)

BASE_URL = "https://courses.finki.ukim.mk"


def _config(*, threads: int = 1) -> InstanceHttpConfig:
    return InstanceHttpConfig(
        base_url=BASE_URL,
        cookies=InstanceCookies(moodle_session="secret-cookie"),
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
    assert caught.value.outcome_count == TRANSPORT_FAILURE_THRESHOLD
    assert caught.value.minimum_outcomes == TRANSPORT_FAILURE_THRESHOLD


def test_transport_failure_threshold_shuts_down_without_waiting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given enough completed failures to abort while another request is pending.
    completed: list[Future[ProfileTransportFailure]] = []
    for profile_id in range(1, TRANSPORT_FAILURE_THRESHOLD + 1):
        future: Future[ProfileTransportFailure] = Future()
        future.set_result(ProfileTransportFailure(profile_id))
        completed.append(future)
    pending: Future[ProfileTransportFailure] = Future()
    executor = Mock()
    executor.submit.side_effect = [*completed, pending]
    monkeypatch.setattr(
        profile_collection,
        "ThreadPoolExecutor",
        Mock(return_value=executor),
    )
    monkeypatch.setattr(profile_collection, "as_completed", lambda _futures: completed)

    # When the observed transport failures exceed the abort threshold.
    with pytest.raises(TransportFailureLimitError):
        scraper.get_profiles(
            _config(threads=4),
            list(range(1, TRANSPORT_FAILURE_THRESHOLD + 2)),
        )

    # Then pending work is cancelled without waiting for a stuck worker.
    executor.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
    assert pending.cancelled()


def test_transport_failure_threshold_uses_observed_rate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given three transport failures interleaved with seven successful outcomes.
    monkeypatch.setattr(scraper, "create_session", lambda _config: requests.Session())
    monkeypatch.setattr(profile_collection, "as_completed", lambda futures: futures)
    failed_ids = frozenset({1, 4, 7})

    def fetch(
        _session: requests.Session,
        profile_id: int,
        _config: InstanceHttpConfig,
    ) -> ProfileTransportFailure | ProfileSuccess:
        if profile_id in failed_ids:
            return ProfileTransportFailure(profile_id)
        return ProfileSuccess({"ID": str(profile_id)})

    monkeypatch.setattr(scraper, "fetch_profile", fetch)

    # When the instance collection observes the complete result stream.
    profiles = scraper.get_profiles(_config(), range(1, 11))

    # Then the low transport-failure rate does not trigger an outage abort.
    assert len(profiles) == 7


def test_transport_failure_rate_is_checked_after_final_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a completed small batch with two failures followed by one success.
    monkeypatch.setattr(scraper, "create_session", lambda _config: requests.Session())
    monkeypatch.setattr(profile_collection, "as_completed", lambda futures: futures)

    def fetch(
        _session: requests.Session,
        profile_id: int,
        _config: InstanceHttpConfig,
    ) -> ProfileTransportFailure | ProfileSuccess:
        if profile_id < 3:
            return ProfileTransportFailure(profile_id)
        return ProfileSuccess({"ID": str(profile_id)})

    monkeypatch.setattr(scraper, "fetch_profile", fetch)

    # When collection observes the success that completes the rate sample.
    with pytest.raises(TransportFailureLimitError) as caught:
        scraper.get_profiles(_config(), [1, 2, 3])

    # Then the completed 2/3 failure rate aborts with accurate diagnostics.
    assert "2 of 3 observed outcomes" in str(caught.value)


def test_transport_failure_rate_is_checked_after_final_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a completed batch with two failures followed by an empty profile.
    monkeypatch.setattr(scraper, "create_session", lambda _config: requests.Session())
    monkeypatch.setattr(profile_collection, "as_completed", lambda futures: futures)

    def fetch(
        _session: requests.Session,
        profile_id: int,
        _config: InstanceHttpConfig,
    ) -> ProfileTransportFailure | ProfileEmpty:
        if profile_id < 3:
            return ProfileTransportFailure(profile_id)
        return ProfileEmpty(profile_id, EmptyReason.EMPTY_PROFILE, 200)

    monkeypatch.setattr(scraper, "fetch_profile", fetch)

    # When collection observes the empty outcome that completes the sample.
    with pytest.raises(TransportFailureLimitError):
        scraper.get_profiles(_config(), [1, 2, 3])


def test_submission_failure_shuts_down_owned_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given an executor that rejects profile submission.
    executor = Mock()
    executor.submit.side_effect = RuntimeError("submission failed")
    monkeypatch.setattr(
        profile_collection,
        "ThreadPoolExecutor",
        Mock(return_value=executor),
    )

    # When profile collection attempts to submit work.
    with pytest.raises(RuntimeError, match="submission failed"):
        scraper.get_profiles(_config(), [1])

    # Then its owned executor is deterministically shut down.
    executor.shutdown.assert_called_once_with(wait=True)


def test_interrupt_closes_created_worker_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a worker session created before result iteration is interrupted.
    session = Mock(spec=requests.Session)
    executor = Mock()

    def submit(
        fetch: Callable[[int], ProfileEmpty],
        profile_id: int,
    ) -> Future[ProfileEmpty]:
        future: Future[ProfileEmpty] = Future()
        future.set_result(fetch(profile_id))
        return future

    executor.submit.side_effect = submit
    monkeypatch.setattr(
        profile_collection,
        "ThreadPoolExecutor",
        Mock(return_value=executor),
    )
    monkeypatch.setattr(scraper, "create_session", Mock(return_value=session))
    monkeypatch.setattr(
        scraper,
        "fetch_profile",
        lambda _session, profile_id, _config: ProfileEmpty(
            profile_id,
            EmptyReason.EMPTY_PROFILE,
            200,
        ),
    )
    monkeypatch.setattr(
        profile_collection,
        "as_completed",
        Mock(side_effect=KeyboardInterrupt),
    )

    # When result collection is interrupted.
    with pytest.raises(KeyboardInterrupt):
        scraper.get_profiles(_config(), [1])

    # Then nonblocking shutdown still releases every created session.
    executor.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
    session.close.assert_called_once_with()


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
    assert caught.value.outcome_count == 2
    assert caught.value.minimum_outcomes == TRANSPORT_FAILURE_THRESHOLD
    assert "2 of 2 observed outcomes" in str(caught.value)
