from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
import requests

import app.__main__ as scraper
from app import coordinator
from app.checkpoints import CheckpointPaths, CheckpointSnapshot, InstanceCheckpoint
from app.checkpoints import load as load_checkpoint
from app.checkpoints import save as save_checkpoint
from app.constants import COL_ID, columns
from app.http import ProfileSuccess
from tests.checkpoint_helpers import make_config


def test_fresh_scrape_never_requests_divergent_old_ids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given requested IDs straddling the first divergent account.
    config = make_config(tmp_path)
    monkeypatch.setattr(coordinator, "preflight_instance", Mock())
    monkeypatch.setattr(
        coordinator, "create_session", lambda _config: requests.Session()
    )
    fetch = Mock(
        side_effect=lambda _session, profile_id, _config: ProfileSuccess(
            {COL_ID: str(profile_id)}
        )
    )
    monkeypatch.setattr(coordinator, "fetch_profile", fetch)
    requested = [16338, 16339, 16340]

    # When the real scheduling and checkpoint path runs.
    new, old = scraper._scrape_with_interrupt_handling(config, requested, requested)

    # Then only the new instance receives IDs at or beyond the cutoff.
    assert set(new[COL_ID]) == {"16338", "16339", "16340"}
    assert old[COL_ID].tolist() == ["16338"]
    old_requests = [
        call.args[1]
        for call in fetch.call_args_list
        if call.args[2].base_url == config.http_old.base_url
    ]
    assert old_requests == [16338]
    saved = load_checkpoint(CheckpointPaths.for_directory(tmp_path), tuple(requested))
    assert saved is not None
    assert saved.requested_ids == tuple(requested)
    assert saved.old.completed_ids == frozenset({16338})


def test_resume_does_not_requeue_excluded_old_ids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a completed new side and an old side complete only below the cutoff.
    config = make_config(tmp_path)
    row = dict.fromkeys(columns, "")
    new = pd.DataFrame([row | {COL_ID: str(i)} for i in [16338, 16339]], dtype="string")
    old = pd.DataFrame([row | {COL_ID: "16338"}], dtype="string")
    snapshot = CheckpointSnapshot(
        (16338, 16339),
        InstanceCheckpoint(new, frozenset({16338, 16339})),
        InstanceCheckpoint(old, frozenset({16338})),
    )
    save_checkpoint(CheckpointPaths.for_directory(tmp_path), snapshot)
    scrape = Mock(side_effect=AssertionError("Unexpected scrape"))
    monkeypatch.setattr(scraper, "_scrape_with_interrupt_handling", scrape)

    # When the same request is resumed.
    resumed_new, resumed_old = scraper._resume_from_checkpoints(config, [16338, 16339])

    # Then skipped archive IDs do not cause perpetual incomplete work.
    scrape.assert_not_called()
    pd.testing.assert_frame_equal(resumed_new, new)
    pd.testing.assert_frame_equal(resumed_old, old)
