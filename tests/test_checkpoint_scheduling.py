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
