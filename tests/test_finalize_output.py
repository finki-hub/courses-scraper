from pathlib import Path

import pandas as pd
import pytest

import app.__main__ as scraper
from app.checkpoints import CheckpointPaths
from app.constants import COL_ID, base_urls, columns, selectors_new, selectors_old
from app.export import EmptyExportError
from app.http import InstanceHttpConfig


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


def test_empty_finalization_preserves_prior_output_and_checkpoints(
    tmp_path: Path,
) -> None:
    # Given prior output and resumable checkpoints.
    config = _config(tmp_path)
    output = tmp_path / "profiles.csv"
    output.write_text("prior output", encoding="utf-8")
    config.checkpoint_new.write_text("new checkpoint", encoding="utf-8")
    config.checkpoint_old.write_text("old checkpoint", encoding="utf-8")

    # When both raw frames would produce an empty final export.
    with pytest.raises(EmptyExportError):
        scraper._finalize_output(
            pd.DataFrame(),
            pd.DataFrame(),
            tmp_path,
            output.name,
            config,
        )

    # Then recovery data and the prior final output remain untouched.
    assert output.read_text(encoding="utf-8") == "prior output"
    assert config.checkpoint_new.read_text(encoding="utf-8") == "new checkpoint"
    assert config.checkpoint_old.read_text(encoding="utf-8") == "old checkpoint"


def test_final_write_failure_preserves_checkpoints(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given valid merged data and existing checkpoints.
    config = _config(tmp_path)
    config.checkpoint_new.write_text("new checkpoint", encoding="utf-8")
    config.checkpoint_old.write_text("old checkpoint", encoding="utf-8")
    row = dict.fromkeys(columns, "")
    row[COL_ID] = "1"

    def fail_write(_frame: pd.DataFrame, _destination: Path) -> None:
        raise OSError("final write fault")

    monkeypatch.setattr(scraper, "export_profiles", fail_write)

    # When the final durable write fails.
    with pytest.raises(OSError, match="final write fault"):
        scraper._finalize_output(
            pd.DataFrame([row]),
            pd.DataFrame(columns=columns),
            tmp_path,
            "profiles.csv",
            config,
        )

    # Then checkpoints remain available for recovery.
    assert config.checkpoint_new.exists()
    assert config.checkpoint_old.exists()


def test_successful_final_write_precedes_checkpoint_clear(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given valid merged data and observable finalization dependencies.
    config = _config(tmp_path)
    row = dict.fromkeys(columns, "")
    row[COL_ID] = "1"
    events: list[str] = []

    def write(_frame: pd.DataFrame, _destination: Path) -> None:
        events.append("write")

    def clear(_paths: CheckpointPaths) -> None:
        events.append("clear")

    monkeypatch.setattr(scraper, "export_profiles", write)
    monkeypatch.setattr(scraper, "clear_checkpoints", clear)

    # When finalization succeeds.
    scraper._finalize_output(
        pd.DataFrame([row]),
        pd.DataFrame(columns=columns),
        tmp_path,
        "profiles.csv",
        config,
    )

    # Then checkpoint clearing occurs only after the durable write returns.
    assert events == ["write", "clear"]


def test_successful_finalization_writes_sanitized_csv_and_clears_checkpoints(
    tmp_path: Path,
) -> None:
    # Given raw profile data and legacy checkpoints.
    config = _config(tmp_path)
    config.checkpoint_new.write_text("new checkpoint", encoding="utf-8")
    config.checkpoint_old.write_text("old checkpoint", encoding="utf-8")
    row = dict.fromkeys(columns, "")
    row.update({COL_ID: "1", "Name": " =formula"})
    raw_old = pd.DataFrame([row])

    # When finalization writes through the real atomic export boundary.
    scraper._finalize_output(
        raw_old,
        pd.DataFrame(columns=columns),
        tmp_path,
        "profiles.csv",
        config,
    )

    # Then the final CSV is safe, raw data is unchanged, and checkpoints are gone.
    exported = pd.read_csv(tmp_path / "profiles.csv", keep_default_na=False)
    assert exported.loc[0, "Name"] == "'=formula"
    assert raw_old.loc[0, "Name"] == " =formula"
    assert not config.checkpoint_new.exists()
    assert not config.checkpoint_old.exists()
