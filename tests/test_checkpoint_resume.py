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
from app.cli import CliConfig
from app.constants import (
    COL_ID,
    base_urls,
    columns,
    selectors_new,
    selectors_old,
)
from app.http import (
    TRANSPORT_FAILURE_THRESHOLD,
    EmptyReason,
    InstanceHttpConfig,
    PreflightError,
    PreflightFailureReason,
    ProfileEmpty,
    ProfileFetchOutcome,
    ProfileSuccess,
    ProfileTransportFailure,
)


def _config(tmp_path: Path) -> scraper.ScrapeConfig:
    return scraper.ScrapeConfig(
        http_new=InstanceHttpConfig(
            base_url=base_urls["new"],
            cookie="new-cookie",
            selectors=selectors_new,
            threads=1,
        ),
        http_old=InstanceHttpConfig(
            base_url=base_urls["old"],
            cookie="old-cookie",
            selectors=selectors_old,
            threads=1,
        ),
        checkpoint_new=tmp_path / "checkpoint_new.csv",
        checkpoint_old=tmp_path / "checkpoint_old.csv",
    )


def test_remaining_profile_ids_are_instance_specific() -> None:
    # Given checkpoints with different completed IDs.
    requested = [1, 2, 3]
    checkpoint_new = pd.DataFrame({COL_ID: [1, 2]})
    checkpoint_old = pd.DataFrame({COL_ID: [1]})

    # When remaining IDs are calculated for each instance.
    remaining_new = scraper._remaining_profile_ids(requested, checkpoint_new)
    remaining_old = scraper._remaining_profile_ids(requested, checkpoint_old)

    # Then one instance never suppresses work for the other.
    assert remaining_new == [3]
    assert remaining_old == [2, 3]


def test_resume_preserves_checkpoint_text(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given checkpoints containing numeric-looking and NA-like text.
    config = _config(tmp_path)
    row = dict.fromkeys(columns, "")
    row.update({COL_ID: "1", "Skype": "00123", "City": "NA"})
    pd.DataFrame([row]).to_csv(config.checkpoint_new, index=False)
    pd.DataFrame([row]).to_csv(config.checkpoint_old, index=False)
    scrape = Mock(return_value=(pd.DataFrame([row]), pd.DataFrame([row])))
    monkeypatch.setattr(scraper, "_scrape_with_interrupt_handling", scrape)

    # When checkpoint resume loads the stored profiles.
    scraper._resume_from_checkpoints(config, [1, 2])

    # Then every scraped field remains exact text.
    state = scrape.call_args.args[3]
    assert state.new.frame.loc[0, "Skype"] == "00123"
    assert state.old.frame.loc[0, "City"] == "NA"


def test_main_resumes_when_only_one_checkpoint_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given one surviving instance checkpoint.
    monkeypatch.chdir(tmp_path)
    output = tmp_path / "output"
    output.mkdir()
    row = dict.fromkeys(columns, "")
    row[COL_ID] = "1"
    pd.DataFrame([row]).to_csv(output / "checkpoint_new.csv", index=False)
    monkeypatch.setattr(
        scraper,
        "parse_cli",
        Mock(
            return_value=CliConfig(
                cookie_new="x",
                cookie_old="y",
                output_file=Path("output/out.csv"),
                threads=1,
                profile_ids=(1, 2),
            ),
        ),
    )
    scrape = Mock(return_value=(pd.DataFrame([row]), pd.DataFrame([row])))
    monkeypatch.setattr(scraper, "_scrape_with_interrupt_handling", scrape)
    monkeypatch.setattr(scraper, "_finalize_output", Mock())

    # When the CLI runs.
    scraper.main()

    # Then the surviving side is supplied as resume state.
    assert scrape.call_args.args[1] == [2]
    assert scrape.call_args.args[2] == [1, 2]
    state = scrape.call_args.args[3]
    assert state.new.frame.loc[0, COL_ID] == "1"
    assert state.old.frame.empty
    runtime_config = scrape.call_args.args[0]
    assert runtime_config.http_new.cookie == "x"
    assert runtime_config.http_old.cookie == "y"
    assert runtime_config.http_new.threads == 1


def test_empty_completes_id_but_transport_failure_does_not(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    monkeypatch.setattr(coordinator, "preflight_instance", Mock())
    monkeypatch.setattr(
        coordinator,
        "create_session",
        lambda _config: requests.Session(),
    )

    def fetch(
        _session: requests.Session,
        profile_id: int,
        http_config: InstanceHttpConfig,
    ) -> ProfileFetchOutcome:
        if http_config is config.http_new and profile_id == 1:
            return ProfileEmpty(profile_id, EmptyReason.EMPTY_PROFILE, 200)
        if http_config is config.http_new:
            return ProfileTransportFailure(profile_id)
        return ProfileSuccess({COL_ID: str(profile_id)})

    monkeypatch.setattr(coordinator, "fetch_profile", fetch)
    terminated = Mock()
    monkeypatch.setattr(scraper, "_terminate_process", terminated)

    with pytest.raises(coordinator.IncompleteScrapeError) as caught:
        scraper._scrape_with_interrupt_handling(config, [1, 2], [1, 2])

    snapshot = load(CheckpointPaths.for_directory(tmp_path), (1, 2))
    assert snapshot is not None
    assert snapshot.new.completed_ids == frozenset({1})
    assert snapshot.old.completed_ids == frozenset({1, 2})
    assert caught.value.remaining_new == frozenset({2})
    terminated.assert_not_called()


def test_preflight_error_propagates_without_save_or_termination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
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


def test_dirty_checkpoint_saves_after_thirty_seconds_below_batch_threshold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
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
    config = _config(tmp_path)
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


def test_resume_without_remaining_work_still_saves_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    row = dict.fromkeys(columns, "")
    row[COL_ID] = "1"
    pd.DataFrame([row]).to_csv(config.checkpoint_new, index=False)
    pd.DataFrame([row]).to_csv(config.checkpoint_old, index=False)
    save = Mock()
    monkeypatch.setattr(scraper, "save_checkpoint", save)
    preflight = Mock()
    monkeypatch.setattr(coordinator, "preflight_instance", preflight)

    scraper._resume_from_checkpoints(config, [1])

    save.assert_called_once()
    preflight.assert_not_called()


def test_interrupt_shuts_down_nonblocking_then_saves_before_termination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
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


def test_fatal_worker_error_saves_before_nonblocking_termination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
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

    scraper._scrape_with_interrupt_handling(config, [1], [])

    executor.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
    assert events == ["save", "terminate:1"]


def test_transport_failure_threshold_aborts_nonblocking_after_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
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


def test_periodic_checkpoint_failure_aborts_nonblocking_and_terminates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
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
    config = _config(tmp_path)
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


def test_abort_terminates_when_salvage_checkpoint_fails_and_keeps_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
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


def test_terminate_process_flushes_logging_and_streams_before_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    stdout = Mock()
    stderr = Mock()
    stdout.flush.side_effect = lambda: events.append("stdout")
    stderr.flush.side_effect = lambda: events.append("stderr")
    monkeypatch.setattr(logging, "shutdown", lambda: events.append("logging"))
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)
    monkeypatch.setattr(os, "_exit", lambda code: events.append(f"exit:{code}"))

    scraper._terminate_process(7)

    assert events == ["logging", "stdout", "stderr", "exit:7"]
