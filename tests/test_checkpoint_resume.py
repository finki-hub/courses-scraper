from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
import requests

import app.__main__ as scraper
from app.constants import COL_ID, columns


def _config(tmp_path: Path) -> scraper.ScrapeConfig:
    return scraper.ScrapeConfig(
        session_new=requests.Session(),
        session_old=requests.Session(),
        threads=1,
        checkpoint_new=tmp_path / "checkpoint_new.csv",
        checkpoint_old=tmp_path / "checkpoint_old.csv",
    )


def test_resolve_profile_ids_deduplicates_explicit_ids() -> None:
    # Given repeated explicit profile IDs.
    args = argparse.Namespace(i=[2, 2, 1, 2], m=None)

    # When the CLI profile IDs are resolved.
    profile_ids = scraper._resolve_profile_ids(args)

    # Then each ID is retained once in its original order.
    assert profile_ids == [2, 1]


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
    assert scrape.call_args.kwargs["existing_new"].loc[0, "Skype"] == "00123"
    assert scrape.call_args.kwargs["existing_old"].loc[0, "City"] == "NA"


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
        "parse_args",
        Mock(
            return_value=argparse.Namespace(
                c1="x",
                c2="y",
                o="out.csv",
                t=1,
                i=[1, 2],
                m=None,
            ),
        ),
    )
    monkeypatch.setattr(
        scraper,
        "get_courses_session",
        Mock(return_value=requests.Session()),
    )
    scrape = Mock(return_value=(pd.DataFrame([row]), pd.DataFrame([row])))
    monkeypatch.setattr(scraper, "_scrape_with_interrupt_handling", scrape)
    monkeypatch.setattr(scraper, "_finalize_output", Mock())

    # When the CLI runs.
    scraper.main()

    # Then the surviving side is supplied as resume state.
    assert scrape.call_args.args[1] == [2]
    assert scrape.call_args.args[2] == [1, 2]
    assert scrape.call_args.kwargs["existing_new"].loc[0, COL_ID] == "1"
    assert scrape.call_args.kwargs["existing_old"].empty
