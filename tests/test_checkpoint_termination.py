from __future__ import annotations

import logging
import os
import sys
from concurrent.futures import Future
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
import requests

import app.__main__ as scraper
from app import coordinator
from app.checkpoints import (
    CheckpointPaths,
    CheckpointSnapshot,
    InstanceCheckpoint,
    load,
)
from app.checkpoints import save as persist_checkpoint
from app.constants import COL_ID, columns
from app.http import (
    ProfileFailureReason,
    ProfileFetchOutcome,
    ProfileRequestError,
    ProfileSuccess,
)
from tests.checkpoint_helpers import make_config


def test_interrupt_shuts_down_nonblocking_then_saves_before_termination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    completed: Future[ProfileFetchOutcome] = Future()
    completed.set_result(ProfileSuccess({COL_ID: "1"}))
    pending: Future[ProfileFetchOutcome] = Future()
    new_executor = Mock()
    old_executor = Mock()
    new_executor.submit.side_effect = [completed]
    old_executor.submit.side_effect = [pending]
    monkeypatch.setattr(
        coordinator,
        "ThreadPoolExecutor",
        Mock(side_effect=[new_executor, old_executor]),
    )
    monkeypatch.setattr(coordinator, "preflight_instance", Mock())
    monkeypatch.setattr(
        coordinator,
        "create_session",
        lambda _config: requests.Session(),
    )
    monkeypatch.setattr(coordinator, "wait", Mock(side_effect=KeyboardInterrupt))
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

    scraper._scrape_with_interrupt_handling(config, [1], [2])

    new_executor.shutdown.assert_called_once_with(
        wait=False,
        cancel_futures=True,
    )
    old_executor.shutdown.assert_called_once_with(
        wait=False,
        cancel_futures=True,
    )
    assert pending.cancelled()
    assert events == ["save", "terminate:130"]


def test_interrupt_after_submission_salvages_completed_profile(
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
    save = Mock()
    terminate = Mock()
    monkeypatch.setattr(coordinator, "save_checkpoint", save)

    snapshot = coordinator.run(
        coordinator.CoordinatorPlan(
            http_new=config.http_new,
            http_old=config.http_old,
            profile_ids_new=[1],
            profile_ids_old=[],
            paths=CheckpointPaths.for_directory(tmp_path),
            initial=None,
            batch_size=100,
            poll_interval=0.25,
            checkpoint_interval=30.0,
            monotonic_clock=Mock(side_effect=KeyboardInterrupt),
            terminate=terminate,
        ),
    )

    assert snapshot.new.completed_ids == frozenset({1})
    executor.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
    save.assert_called_once()
    terminate.assert_called_once_with(130)


def test_fatal_worker_error_saves_before_nonblocking_termination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    failed: Future[ProfileFetchOutcome] = Future()
    failed.set_exception(
        ProfileRequestError(1, ProfileFailureReason.LOGIN_RESPONSE, 200),
    )
    executor = Mock()
    executor.submit.return_value = failed
    monkeypatch.setattr(coordinator, "ThreadPoolExecutor", Mock(return_value=executor))
    monkeypatch.setattr(coordinator, "preflight_instance", Mock())
    monkeypatch.setattr(
        coordinator,
        "create_session",
        lambda _config: requests.Session(),
    )
    events: list[str] = []
    snapshots: list[CheckpointSnapshot] = []

    def record_save(_paths: CheckpointPaths, snapshot: CheckpointSnapshot) -> None:
        snapshots.append(snapshot)
        events.append("save")

    monkeypatch.setattr(
        coordinator,
        "save_checkpoint",
        record_save,
    )
    monkeypatch.setattr(
        scraper,
        "_terminate_process",
        lambda code: events.append(f"terminate:{code}"),
    )

    scraper._scrape_with_interrupt_handling(config, [1], [])

    executor.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
    assert events == ["save", "terminate:1"]
    assert snapshots[0].new.completed_ids == frozenset()


def test_abort_terminates_when_salvage_checkpoint_fails_and_keeps_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    paths = CheckpointPaths.for_directory(tmp_path)
    empty = pd.DataFrame(columns=columns, dtype="string")
    prior = CheckpointSnapshot(
        (1,),
        InstanceCheckpoint(empty, frozenset()),
        InstanceCheckpoint(empty.copy(), frozenset()),
    )
    persist_checkpoint(paths, prior)
    failed: Future[ProfileFetchOutcome] = Future()
    failed.set_exception(RuntimeError("worker failed"))
    executor = Mock()
    executor.submit.return_value = failed
    monkeypatch.setattr(coordinator, "ThreadPoolExecutor", Mock(return_value=executor))
    monkeypatch.setattr(coordinator, "preflight_instance", Mock())
    monkeypatch.setattr(
        coordinator,
        "create_session",
        lambda _config: requests.Session(),
    )
    monkeypatch.setattr(
        coordinator,
        "save_checkpoint",
        Mock(side_effect=OSError("salvage checkpoint fault")),
    )
    terminated = Mock()
    monkeypatch.setattr(scraper, "_terminate_process", terminated)

    scraper._scrape_with_interrupt_handling(config, [1], [])

    terminated.assert_called_once_with(1)
    loaded = load(paths, (1,))
    assert loaded is not None
    assert loaded.requested_ids == (1,)


def test_terminate_process_does_not_block_on_logging_or_stream_flushes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    stdout = Mock()
    stderr = Mock()
    shutdown = Mock()
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)
    monkeypatch.setattr(logging, "shutdown", shutdown)
    monkeypatch.setattr(os, "_exit", lambda code: events.append(f"exit:{code}"))

    scraper._terminate_process(7)

    assert events == ["exit:7"]
    shutdown.assert_not_called()
    stdout.flush.assert_not_called()
    stderr.flush.assert_not_called()
