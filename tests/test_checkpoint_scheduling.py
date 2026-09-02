from __future__ import annotations

from concurrent.futures import Future
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
import requests

import app.__main__ as scraper
from app import coordinator
from app.checkpoints import CheckpointPaths, CheckpointSnapshot
from app.constants import COL_ID
from app.http import EmptyReason, ProfileEmpty, ProfileFetchOutcome, ProfileSuccess
from tests.checkpoint_helpers import make_config


def test_dirty_checkpoint_saves_after_thirty_seconds_below_batch_threshold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    first: Future[ProfileFetchOutcome] = Future()
    first.set_result(ProfileSuccess({COL_ID: "1"}))
    second: Future[ProfileFetchOutcome] = Future()
    second.set_result(ProfileEmpty(2, EmptyReason.EMPTY_PROFILE, 200))
    executor = Mock()
    executor.submit.side_effect = [first, second]
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
                ({first}, {second}),
                (set(), {second}),
                ({second}, set()),
            ],
        ),
    )
    save = Mock()
    monkeypatch.setattr(coordinator, "save_checkpoint", save)
    clock = Mock(side_effect=[0.0, 0.0, 31.0, 31.0])

    coordinator.run(
        coordinator.CoordinatorPlan(
            http_new=config.http_new,
            http_old=config.http_old,
            profile_ids_new=[1, 2],
            profile_ids_old=[],
            paths=CheckpointPaths.for_directory(tmp_path),
            initial=None,
            batch_size=100,
            poll_interval=0.25,
            checkpoint_interval=30.0,
            monotonic_clock=clock,
            terminate=Mock(),
        ),
    )

    assert save.call_count == 2
    periodic_snapshot = save.call_args_list[0].args[1]
    assert periodic_snapshot.new.completed_ids == frozenset({1})


def test_coordinator_saves_in_batches_and_unconditionally_at_end(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    monkeypatch.setattr(scraper, "CHECKPOINT_BATCH_SIZE", 2)
    monkeypatch.setattr(coordinator, "preflight_instance", Mock())
    monkeypatch.setattr(
        coordinator,
        "create_session",
        lambda _config: requests.Session(),
    )
    monkeypatch.setattr(
        coordinator,
        "fetch_profile",
        lambda _session, profile_id, _config: ProfileSuccess(
            {COL_ID: str(profile_id)},
        ),
    )
    save = Mock()
    monkeypatch.setattr(coordinator, "save_checkpoint", save)

    scraper._scrape_with_interrupt_handling(config, [1, 2], [3])

    assert save.call_count == 2
    final_snapshot = save.call_args.args[1]
    assert isinstance(final_snapshot, CheckpointSnapshot)
    assert final_snapshot.new.completed_ids == frozenset({1, 2})
    assert final_snapshot.old.completed_ids == frozenset({3})


def test_coordinator_saves_each_batch_within_one_completed_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given five completions returned by one executor wait and a batch size of two.
    config = make_config(tmp_path)
    futures: list[Future[ProfileFetchOutcome]] = []
    for profile_id in range(1, 6):
        future: Future[ProfileFetchOutcome] = Future()
        future.set_result(ProfileSuccess({COL_ID: str(profile_id)}))
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
    monkeypatch.setattr(coordinator, "wait", Mock(return_value=(futures, set())))
    save = Mock()
    monkeypatch.setattr(coordinator, "save_checkpoint", save)

    # When the coordinator consumes the completed set, then each full batch is durable.
    coordinator.run(
        coordinator.CoordinatorPlan(
            http_new=config.http_new,
            http_old=config.http_old,
            profile_ids_new=[1, 2, 3, 4, 5],
            profile_ids_old=[],
            paths=CheckpointPaths.for_directory(tmp_path),
            initial=None,
            batch_size=2,
            poll_interval=0.25,
            checkpoint_interval=30.0,
            monotonic_clock=Mock(return_value=0.0),
            terminate=Mock(),
        ),
    )

    assert [len(call.args[1].new.completed_ids) for call in save.call_args_list] == [
        2,
        4,
        5,
    ]


def test_coordinator_reports_each_completed_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given two completed profile requests and an observable progress sink.
    config = make_config(tmp_path)
    futures: list[Future[ProfileFetchOutcome]] = []
    for profile_id in (1, 2):
        future: Future[ProfileFetchOutcome] = Future()
        future.set_result(ProfileSuccess({COL_ID: str(profile_id)}))
        futures.append(future)
    executor = Mock()
    executor.submit.side_effect = futures
    progress = Mock()
    progress_context = MagicMock()
    progress_context.__enter__.return_value = progress
    progress_factory = Mock(return_value=progress_context)
    monkeypatch.setattr(coordinator, "ThreadPoolExecutor", Mock(return_value=executor))
    monkeypatch.setattr(coordinator, "preflight_instance", Mock())
    monkeypatch.setattr(
        coordinator, "create_session", lambda _config: requests.Session()
    )
    monkeypatch.setattr(coordinator, "tqdm", progress_factory, raising=False)

    # When scraping completes, then each completed request advances progress once.
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
            Mock(return_value=0.0),
            Mock(),
        ),
    )

    progress_factory.assert_called_once_with(total=2)
    assert progress.update.call_count == 2
