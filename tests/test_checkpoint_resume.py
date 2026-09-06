from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

import app.__main__ as scraper
from app import coordinator
from app.auth import (
    CasCredentials,
    InstanceCookies,
    ManualCookies,
)
from app.cli import CliConfig
from app.constants import COL_ID, columns
from app.http import PreflightError, PreflightFailureReason
from tests.checkpoint_helpers import make_config

CREDENTIAL_VALUE = "credential-value"


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
    config = make_config(tmp_path)
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
                authentication=ManualCookies(
                    new=InstanceCookies(moodle_session="x"),
                    old=InstanceCookies(moodle_session="y"),
                ),
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
    assert runtime_config.http_new.cookies == InstanceCookies(moodle_session="x")
    assert runtime_config.http_old.cookies == InstanceCookies(moodle_session="y")
    assert runtime_config.http_new.threads == 1


def test_main_preflights_cas_cookies_before_checkpoint_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given CAS returns cookies while a checkpoint would otherwise bypass scraping.
    monkeypatch.chdir(tmp_path)
    output = tmp_path / "output"
    output.mkdir()
    (output / "checkpoint_new.csv").write_text("checkpoint", encoding="utf-8")
    credentials = CasCredentials("student", CREDENTIAL_VALUE)
    monkeypatch.setattr(
        scraper,
        "parse_cli",
        Mock(
            return_value=CliConfig(
                authentication=credentials,
                output_file=Path("output/out.csv"),
                threads=1,
                profile_ids=(1,),
            ),
        ),
    )
    monkeypatch.setattr(
        scraper,
        "authenticate_instance",
        Mock(
            side_effect=[
                InstanceCookies("new-session", "new-server"),
                InstanceCookies("old-session", "old-server"),
            ],
        ),
    )
    session = MagicMock()
    session.__enter__.return_value = session
    monkeypatch.setattr(scraper, "create_session", Mock(return_value=session))
    preflight = Mock(
        side_effect=[
            None,
            PreflightError(
                base_url="https://oldcourses.finki.ukim.mk",
                reason=PreflightFailureReason.LOGIN_RESPONSE,
            ),
        ],
    )
    monkeypatch.setattr(scraper, "preflight_instance", preflight, raising=False)
    resume = Mock(side_effect=AssertionError("unexpected resume"))
    finalize = Mock(side_effect=AssertionError("unexpected finalization"))
    monkeypatch.setattr(scraper, "_resume_from_checkpoints", resume)
    monkeypatch.setattr(scraper, "_finalize_output", finalize)

    # When either host rejects its CAS-derived cookies.
    with pytest.raises(PreflightError):
        scraper.main()

    # Then checkpoint resume and final output cannot bypass authentication.
    assert preflight.call_count == 2
    resume.assert_not_called()
    finalize.assert_not_called()
    assert not (output / "out.csv").exists()


def test_resume_without_remaining_work_still_saves_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
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
