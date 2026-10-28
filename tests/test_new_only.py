from pathlib import Path

import pandas as pd
import pytest

from app import __main__ as main
from app.auth import CasCredentials, InstanceCookies, ManualCookies
from app.checkpoints import CheckpointSnapshot, InstanceCheckpoint
from app.cli import CliConfig, parse_cli
from app.constants import COL_ID, base_urls
from app.historical import HistoricalSnapshot


def test_new_only_prompts_only_new_cookie() -> None:
    prompts: list[str] = []

    def secret(prompt: str) -> str:
        prompts.append(prompt)
        return "new-cookie"

    config = parse_cli(
        ["--instances", "new", "--historical", "history.csv", "-i", "1"],
        environ={"COURSES_COOKIE_OLD": "ignored"},
        prompt_secret=secret,
    )
    assert isinstance(config.authentication, ManualCookies)
    assert config.authentication.old is None
    assert config.historical == Path("history.csv")
    assert prompts == ["New Courses MoodleSession cookie: "]


@pytest.mark.parametrize(
    "arguments",
    [
        ["--instances", "new"],
        ["--historical", "history.csv"],
        ["--instances", "new", "--historical", "history.csv", "-c2", "old"],
    ],
)
def test_invalid_modes_rejected_before_credentials(arguments: list[str]) -> None:
    def unexpected_prompt(_prompt: str) -> str:
        pytest.fail("credentials must not be requested")

    with pytest.raises(SystemExit) as error:
        parse_cli([*arguments, "-i", "1"], environ={}, prompt_secret=unexpected_prompt)
    assert error.value.code == 2


def test_new_only_cas_and_isolated_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cli = CliConfig(
        CasCredentials("user", "secret"),
        tmp_path / "result.csv",
        1,
        (1,),
        "new",
        tmp_path / "history.csv",
    )
    authenticated: list[str] = []

    def authenticate(_credentials: CasCredentials, url: str) -> InstanceCookies:
        authenticated.append(url)
        return InstanceCookies("new-cookie")

    monkeypatch.setattr(main, "authenticate_instance", authenticate)
    new, old, verify = main._authenticate(cli)
    snapshot = HistoricalSnapshot(pd.DataFrame(), tmp_path / "history.csv", "a" * 64)
    config = main._build_config(cli, new, old, snapshot)
    assert authenticated == [base_urls["new"]]
    assert verify
    assert old is None
    assert config.http_old is None
    assert config.checkpoint_new.parent == tmp_path / ".checkpoints-new-only" / (
        "a" * 64
    )


def test_history_output_collision_precedes_auth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "result.csv"
    destination.write_text("untouched", encoding="utf-8")
    cli = CliConfig(
        ManualCookies(InstanceCookies("new"), None),
        destination,
        1,
        (1,),
        "new",
        destination,
    )
    monkeypatch.setattr(main, "parse_cli", lambda: cli)
    with pytest.raises(ValueError, match="output destination"):
        main.main()
    assert destination.read_text(encoding="utf-8") == "untouched"


def test_resume_new_only_never_requeues_old(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cli = CliConfig(
        ManualCookies(InstanceCookies("new"), None),
        tmp_path / "result.csv",
        1,
        (1, 2),
        "new",
        tmp_path / "history.csv",
    )
    history = HistoricalSnapshot(pd.DataFrame(), tmp_path / "history.csv", "c" * 64)
    config = main._build_config(cli, InstanceCookies("new"), None, history)
    empty = pd.DataFrame(columns=[COL_ID])
    checkpoint = CheckpointSnapshot(
        (1, 2),
        InstanceCheckpoint(empty, frozenset({1})),
        InstanceCheckpoint(empty, frozenset()),
    )
    monkeypatch.setattr(main, "load_checkpoint", lambda *_args: checkpoint)

    def scrape(
        _config: main.ScrapeConfig, new: object, old: object, state: object
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        assert new == [2]
        assert old == []
        assert state is not None
        return empty, empty

    monkeypatch.setattr(main, "_scrape_with_interrupt_handling", scrape)
    main._resume_from_checkpoints(config, (1, 2))


def test_empty_new_run_retains_destination_and_checkpoints(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "result.csv"
    destination.write_text("untouched", encoding="utf-8")
    cli = CliConfig(
        ManualCookies(InstanceCookies("new"), None),
        destination,
        1,
        (1,),
        "new",
        tmp_path / "history.csv",
    )
    snapshot = HistoricalSnapshot(pd.DataFrame(), tmp_path / "history.csv", "b" * 64)
    config = main._build_config(cli, InstanceCookies("new"), None, snapshot)
    old_work: list[int] = []

    def scrape(
        _config: main.ScrapeConfig, _new: object, old: object
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        assert old == ()
        assert not old_work
        return pd.DataFrame(), pd.DataFrame()

    monkeypatch.setattr(main, "_scrape_with_interrupt_handling", scrape)
    with pytest.raises(ValueError, match="no fresh profiles"):
        main._run_and_export(cli, config, snapshot, False)
    assert destination.read_text(encoding="utf-8") == "untouched"


@pytest.mark.parametrize("export_fails", [False, True])
def test_combined_export_clears_only_after_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, export_fails: bool
) -> None:
    cli = CliConfig(
        ManualCookies(InstanceCookies("new"), None),
        tmp_path / "result.csv",
        1,
        (1,),
        "new",
        tmp_path / "history.csv",
    )
    history = HistoricalSnapshot(
        pd.DataFrame({"historical": ["retained"]}), tmp_path / "history.csv", "d" * 64
    )
    config = main._build_config(cli, InstanceCookies("new"), None, history)
    fresh = pd.DataFrame({COL_ID: ["1"]})
    combined = pd.DataFrame({"combined": ["retained", "fresh"]})
    events: list[str] = []
    monkeypatch.setattr(
        main, "_scrape_with_interrupt_handling", lambda *_args: (fresh, pd.DataFrame())
    )

    def merge(snapshot: HistoricalSnapshot, new: pd.DataFrame) -> pd.DataFrame:
        assert snapshot is history
        assert new is fresh
        return combined

    def export(frame: pd.DataFrame, destination: Path) -> None:
        assert frame is combined
        assert destination == cli.output_file
        events.append("export")
        if export_fails:
            raise OSError("simulated export failure")

    def clear(paths: object) -> None:
        assert paths == main._checkpoint_paths(config)
        events.append("clear")

    monkeypatch.setattr(main, "merge_historical_snapshot", merge)
    monkeypatch.setattr(main, "export_profiles", export)
    monkeypatch.setattr(main, "clear_checkpoints", clear)
    if export_fails:
        with pytest.raises(OSError, match="simulated"):
            main._run_and_export(cli, config, history, False)
        assert events == ["export"]
    else:
        main._run_and_export(cli, config, history, False)
        assert events == ["export", "clear"]
