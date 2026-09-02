from __future__ import annotations

from concurrent.futures import Future
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

import app.__main__ as scraper
from app import coordinator
from app.checkpoints import CheckpointPaths, CheckpointSnapshot
from app.constants import COL_ID
from app.http import (
    TRANSPORT_FAILURE_THRESHOLD,
    PreflightError,
    PreflightFailureReason,
    ProfileFetchOutcome,
    ProfileSuccess,
    ProfileTransportFailure,
)
from tests.checkpoint_helpers import make_config


def test_preflight_error_propagates_without_save_or_termination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    error = PreflightError(
        base_url=config.http_new.base_url,
        reason=PreflightFailureReason.TRANSPORT,
    )
    monkeypatch.setattr(coordinator, "preflight_instance", Mock(side_effect=error))
    monkeypatch.setattr(
        coordinator,
        "create_session",
        lambda _config: requests.Session(),
    )
    save = Mock()
    terminate = Mock()
    executor_factory = Mock()
    monkeypatch.setattr(coordinator, "save_checkpoint", save)
    monkeypatch.setattr(coordinator, "ThreadPoolExecutor", executor_factory)
    monkeypatch.setattr(scraper, "_terminate_process", terminate)

    with pytest.raises(PreflightError) as caught:
        scraper._scrape_with_interrupt_handling(config, [1], [1])

    assert caught.value is error
    save.assert_not_called()
    terminate.assert_not_called()
    executor_factory.assert_not_called()


def test_transport_failure_threshold_aborts_nonblocking_after_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    futures: list[Future[ProfileFetchOutcome]] = []
    for profile_id in range(1, TRANSPORT_FAILURE_THRESHOLD + 1):
        future: Future[ProfileFetchOutcome] = Future()
        future.set_result(ProfileTransportFailure(profile_id))
        futures.append(future)
    executor = Mock()
    executor.submit.side_effect = futures
    monkeypatch.setattr(coordinator, "ThreadPoolExecutor", Mock(return_value=executor))
    monkeypatch.setattr(coordinator, "preflight_instance", Mock())
    monkeypatch.setattr(
        coordinator,
        "create_session",
        lambda _config: requests.Session(),
    )
    events: list[str] = []
    monkeypatch.setattr(
        coordinator,
        "save_checkpoint",
        lambda _paths, _snapshot: events.append("save"),
    )
    monkeypatch.setattr(
        scraper,
        "_terminate_process",
        lambda code: events.append(f"terminate:{code}"),
    )

    scraper._scrape_with_interrupt_handling(
        config,
        list(range(1, TRANSPORT_FAILURE_THRESHOLD + 1)),
        [],
    )

    executor.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
    assert events == ["save", "terminate:1"]


def test_coordinator_checks_failure_rate_after_final_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given two transport failures followed by a success completing the sample.
    config = make_config(tmp_path)
    outcomes: tuple[ProfileFetchOutcome, ...] = (
        ProfileTransportFailure(1),
        ProfileTransportFailure(2),
        ProfileSuccess({COL_ID: "3"}),
    )
    futures: list[Future[ProfileFetchOutcome]] = []
    for outcome in outcomes:
        future: Future[ProfileFetchOutcome] = Future()
        future.set_result(outcome)
        futures.append(future)
    executor = Mock()
    executor.submit.side_effect = futures
    monkeypatch.setattr(coordinator, "ThreadPoolExecutor", Mock(return_value=executor))
    monkeypatch.setattr(coordinator, "preflight_instance", Mock())
    monkeypatch.setattr(
        coordinator,
        "create_session",
        lambda _config: requests.Session(),
    )
    monkeypatch.setattr(
        coordinator,
        "wait",
        Mock(
            side_effect=[
                ({futures[0]}, {futures[1], futures[2]}),
                ({futures[1]}, {futures[2]}),
                ({futures[2]}, set()),
            ],
        ),
    )
    save = Mock()
    terminate = Mock()
    monkeypatch.setattr(coordinator, "save_checkpoint", save)

    # When the coordinator observes all three outcomes.
    coordinator.run(
        coordinator.CoordinatorPlan(
            http_new=config.http_new,
            http_old=config.http_old,
            profile_ids_new=[1, 2, 3],
            profile_ids_old=[],
            paths=CheckpointPaths.for_directory(tmp_path),
            initial=None,
            batch_size=100,
            poll_interval=0.25,
            checkpoint_interval=30.0,
            monotonic_clock=Mock(return_value=0.0),
            terminate=terminate,
        ),
    )

    # Then the final 2/3 transport-failure rate triggers salvage and termination.
    terminate.assert_called_once_with(1)
    snapshot = save.call_args.args[1]
    assert snapshot.new.completed_ids == frozenset({3})


def test_periodic_checkpoint_failure_aborts_nonblocking_and_terminates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    completed: Future[ProfileFetchOutcome] = Future()
    completed.set_result(ProfileSuccess({COL_ID: "1"}))
    pending: Future[ProfileFetchOutcome] = Future()
    executor = Mock()
    executor.submit.side_effect = [completed, pending]
    monkeypatch.setattr(coordinator, "ThreadPoolExecutor", Mock(return_value=executor))
    monkeypatch.setattr(coordinator, "preflight_instance", Mock())
    monkeypatch.setattr(
        coordinator,
        "create_session",
        lambda _config: requests.Session(),
    )
    monkeypatch.setattr(
        coordinator,
        "wait",
        Mock(return_value=({completed}, {pending})),
    )
    events: list[str] = []

    def save(_paths: CheckpointPaths, _snapshot: CheckpointSnapshot) -> None:
        events.append("save")
        if events == ["save"]:
            raise OSError("periodic checkpoint fault")

    monkeypatch.setattr(coordinator, "save_checkpoint", save)

    def terminate(code: int) -> None:
        events.append(f"terminate:{code}")

    coordinator.run(
        coordinator.CoordinatorPlan(
            config.http_new,
            config.http_old,
            [1, 2],
            [],
            CheckpointPaths.for_directory(tmp_path),
            None,
            100,
            0.25,
            30.0,
            Mock(side_effect=[0.0, 31.0]),
            terminate,
        ),
    )

    executor.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
    assert pending.cancelled()
    assert events == ["save", "save", "terminate:1"]


def test_final_checkpoint_failure_aborts_nonblocking_and_terminates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    completed: Future[ProfileFetchOutcome] = Future()
    completed.set_result(ProfileSuccess({COL_ID: "1"}))
    executor = Mock()
    executor.submit.return_value = completed
    monkeypatch.setattr(coordinator, "ThreadPoolExecutor", Mock(return_value=executor))
    monkeypatch.setattr(coordinator, "preflight_instance", Mock())
    monkeypatch.setattr(
        coordinator,
        "create_session",
        lambda _config: requests.Session(),
    )
    events: list[str] = []

    def save(_paths: CheckpointPaths, _snapshot: CheckpointSnapshot) -> None:
        events.append("save")
        if events == ["save"]:
            raise OSError("final checkpoint fault")

    monkeypatch.setattr(coordinator, "save_checkpoint", save)
    monkeypatch.setattr(
        scraper,
        "_terminate_process",
        lambda code: events.append(f"terminate:{code}"),
    )

    scraper._scrape_with_interrupt_handling(config, [1], [])

    executor.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
    assert events == ["save", "save", "terminate:1"]
