from __future__ import annotations

from concurrent.futures import Future
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

import app.__main__ as scraper
from app import coordinator
from app.auth import InstanceCookies
from app.constants import COL_ID, base_urls, selectors_new, selectors_old
from app.http import InstanceHttpConfig, ProfileFetchOutcome, ProfileSuccess


def make_config(tmp_path: Path) -> scraper.ScrapeConfig:
    return scraper.ScrapeConfig(
        http_new=InstanceHttpConfig(
            base_url=base_urls["new"],
            cookies=InstanceCookies(moodle_session="new-cookie"),
            selectors=selectors_new,
            threads=1,
        ),
        http_old=InstanceHttpConfig(
            base_url=base_urls["old"],
            cookies=InstanceCookies(moodle_session="old-cookie"),
            selectors=selectors_old,
            threads=1,
        ),
        checkpoint_new=tmp_path / "checkpoint_new.csv",
        checkpoint_old=tmp_path / "checkpoint_old.csv",
    )


def mock_completed_profile_executor(monkeypatch: pytest.MonkeyPatch) -> Mock:
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
    return executor
