from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

import app.__main__ as scraper
from app import coordinator, coordinator_workers
from app.auth import InstanceCookies
from app.checkpoints import CheckpointPaths, load
from app.constants import COL_ID, base_urls, selectors_new
from app.http import (
    EmptyReason,
    InstanceHttpConfig,
    ProfileEmpty,
    ProfileFetchOutcome,
    ProfileSuccess,
    ProfileTransportFailure,
)
from tests.checkpoint_helpers import make_config


def test_empty_completes_id_but_transport_failure_does_not(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
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


def test_worker_submission_failure_releases_owned_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given one started worker session followed by a rejected submission.
    config = InstanceHttpConfig(
        base_url=base_urls["new"],
        cookies=InstanceCookies(moodle_session="cookie"),
        selectors=selectors_new,
        threads=1,
    )
    session = Mock(spec=requests.Session)
    executor = Mock()
    submit_count = 0

    def submit(
        fetch: Callable[[int], ProfileFetchOutcome],
        profile_id: int,
    ) -> Future[ProfileFetchOutcome]:
        nonlocal submit_count
        submit_count += 1
        if submit_count == 2:
            raise RuntimeError("submission failed")
        future: Future[ProfileFetchOutcome] = Future()
        future.set_result(fetch(profile_id))
        return future

    executor.submit.side_effect = submit
    dependencies = coordinator_workers.WorkerDependencies(
        executor_factory=Mock(return_value=executor),
        create_session=Mock(return_value=session),
        fetch_profile=lambda _session, profile_id, _config: ProfileEmpty(
            profile_id,
            EmptyReason.EMPTY_PROFILE,
            200,
        ),
    )

    # When submission fails after the first worker initialized its session.
    with pytest.raises(RuntimeError, match="submission failed"):
        coordinator_workers.submit(config, [1, 2], dependencies)

    # Then the partially initialized executor and session are released.
    executor.shutdown.assert_called_once_with(wait=True, cancel_futures=True)
    session.close.assert_called_once_with()
