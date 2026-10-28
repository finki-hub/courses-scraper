import hashlib
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import pytest
import requests

from app import __main__ as main
from app import coordinator
from app.auth import CasCredentials, InstanceCookies, ManualCookies
from app.checkpoints import CheckpointSnapshot, InstanceCheckpoint
from app.checkpoints import load as load_checkpoint
from app.checkpoints import save as save_checkpoint
from app.cli import CliConfig
from app.constants import base_urls, columns
from app.historical import HistoricalDataError, load_historical_snapshot
from app.http import InstanceHttpConfig
from app.profile_merge import MERGED_COLUMNS
from app.profile_outcomes import ProfileSuccess


def _row(old: str = "", new: str = "", **values: str) -> dict[str, str]:
    return dict.fromkeys(MERGED_COLUMNS, "") | {"ID_old": old, "ID_new": new, **values}


@dataclass
class Pipeline:
    root: Path
    sessions: list[str] = field(default_factory=list)
    preflights: list[str] = field(default_factory=list)
    fetched: list[int] = field(default_factory=list)
    authenticated: list[str] = field(default_factory=list)

    @property
    def source(self) -> Path:
        return self.root / "history.csv"

    @property
    def destination(self) -> Path:
        return self.root / "output" / "profiles.csv"

    def config(self) -> main.ScrapeConfig:
        snapshot = load_historical_snapshot(self.source)
        cli = CliConfig(
            ManualCookies(InstanceCookies("test"), None),
            self.destination,
            1,
            (2, 10),
            "new",
            self.source,
        )
        config = main._build_config(cli, InstanceCookies("test"), None, snapshot)
        assert config.http_old is None
        return config

    def session(self, config: InstanceHttpConfig) -> requests.Session:
        self.sessions.append(config.base_url)
        assert config.base_url == base_urls["new"]
        return requests.Session()

    def preflight(self, _session: requests.Session, config: InstanceHttpConfig) -> None:
        self.preflights.append(config.base_url)
        assert config.base_url == base_urls["new"]

    def fetch(
        self, _session: requests.Session, profile_id: int, config: InstanceHttpConfig
    ) -> ProfileSuccess:
        assert config.base_url == base_urls["new"]
        self.fetched.append(profile_id)
        row = dict.fromkeys(columns, "") | {
            "ID": str(profile_id),
            "Name": f"Fresh {profile_id}",
            "Description": "fresh",
        }
        return ProfileSuccess(row)

    def authenticate(self, _credentials: CasCredentials, url: str) -> InstanceCookies:
        self.authenticated.append(url)
        assert url == base_urls["new"]
        return InstanceCookies("test")


@pytest.fixture
def pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Pipeline:
    return _configure_pipeline(tmp_path, monkeypatch)


def _configure_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Pipeline:
    harness = Pipeline(tmp_path)
    rows = [
        _row(
            "2",
            "2",
            Name="Prior",
            Description_old="frozen",
            Profile_old="old-url",
            Description_new="stale",
        ),
        _row("3", Name="Old only", Description_old="old only"),
        _row(new="20000", Name="Unmatched new", Description_new="unmatched"),
    ]
    pd.DataFrame(rows, columns=list(MERGED_COLUMNS)).to_csv(harness.source, index=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("COURSES_COOKIE_NEW", "test")
    monkeypatch.setenv("COURSES_COOKIE_OLD", "must-not-use")
    monkeypatch.setenv("COURSES_CAS_USERNAME", "test-user")
    monkeypatch.setenv("COURSES_CAS_PASSWORD", "test-password")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "scraper",
            "--instances",
            "new",
            "--historical",
            str(harness.source),
            "-i",
            "2",
            "10",
            "-t",
            "1",
        ],
    )
    monkeypatch.setattr(coordinator, "create_session", harness.session)
    monkeypatch.setattr(coordinator, "preflight_instance", harness.preflight)
    monkeypatch.setattr(coordinator, "fetch_profile", harness.fetch)
    monkeypatch.setattr(main, "create_session", harness.session)
    monkeypatch.setattr(main, "preflight_instance", harness.preflight)
    monkeypatch.setattr(main, "authenticate_instance", harness.authenticate)
    monkeypatch.setattr(main, "_terminate_process", _unexpected_termination)
    return harness


def _unexpected_termination(code: int) -> None:
    pytest.fail(f"Coordinator unexpectedly terminated with code {code}")


def _assert_combined(pipeline: Pipeline, source_bytes: bytes) -> None:
    result = pd.read_csv(pipeline.destination, dtype=str, keep_default_na=False)
    assert tuple(result.columns) == MERGED_COLUMNS
    refreshed = result.loc[result["ID_new"] == "2"].iloc[0]
    assert refreshed["Name"] == "Fresh 2"
    assert refreshed["Description_new"] == "fresh"
    history = pd.read_csv(pipeline.source, dtype=str, keep_default_na=False)
    for old_id in ("2", "3"):
        prior = history.loc[history["ID_old"] == old_id].iloc[0]
        current = result.loc[result["ID_old"] == old_id].iloc[0]
        for column in MERGED_COLUMNS:
            if column.endswith("_old"):
                assert current[column] == prior[column]
    assert result.loc[result["ID_new"] == "20000", "Description_new"].tolist() == [
        "unmatched"
    ]
    assert set(result["ID_new"]) == {"2", "10", "20000", ""}
    assert pipeline.source.read_bytes() == source_bytes


@pytest.mark.parametrize("cas", [False, True])
def test_fresh_real_pipeline(
    pipeline: Pipeline, cas: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    if cas:
        monkeypatch.setattr(sys, "argv", [*sys.argv, "--cas"])
    source_bytes = pipeline.source.read_bytes()
    output = pipeline.destination.parent
    output.mkdir()
    sentinels = [
        output / name
        for name in (
            "checkpoint_new.csv",
            "checkpoint_old.csv",
            "checkpoint_manifest.json",
        )
    ]
    for sentinel in sentinels:
        sentinel.write_bytes(b"both-mode sentinel")
    paths = main._checkpoint_paths(pipeline.config())
    main.main()
    _assert_combined(pipeline, source_bytes)
    assert pipeline.fetched == [2, 10]
    assert pipeline.authenticated == ([base_urls["new"]] if cas else [])
    assert pipeline.preflights == [base_urls["new"]]
    assert not paths.manifest.exists()
    assert all(path.read_bytes() == b"both-mode sentinel" for path in sentinels)


def test_export_failure_then_real_resume(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_bytes = pipeline.source.read_bytes()
    pipeline.destination.parent.mkdir()
    pipeline.destination.write_bytes(b"previous destination")
    paths = main._checkpoint_paths(pipeline.config())

    replace = Path.replace

    def fail_export(source: Path, destination: str | Path) -> Path:
        if Path(destination).resolve() == pipeline.destination:
            raise OSError("simulated export failure")
        return replace(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "replace", fail_export)
        with pytest.raises(OSError, match="simulated export"):
            main.main()
    assert paths.manifest.exists()
    checkpoint = load_checkpoint(paths, (2, 10))
    assert checkpoint is not None
    assert checkpoint.new.completed_ids == frozenset({2, 10})
    assert checkpoint.old.frame.empty
    assert not checkpoint.old.completed_ids
    assert pipeline.destination.read_bytes() == b"previous destination"
    assert pipeline.source.read_bytes() == source_bytes
    main.main()
    assert pipeline.fetched == [2, 10]
    assert not paths.manifest.exists()
    _assert_combined(pipeline, source_bytes)


def test_invalid_history_precedes_auth_and_network(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline.source.write_bytes(b"invalid schema\ninvalid row\n")
    monkeypatch.setattr(sys, "argv", [*sys.argv, "--cas"])
    with pytest.raises(HistoricalDataError):
        main.main()
    assert not pipeline.sessions
    assert not pipeline.authenticated
    assert not pipeline.destination.exists()


def test_changed_history_uses_separate_checkpoint_identity(pipeline: Pipeline) -> None:
    first = main._checkpoint_paths(pipeline.config())
    first.manifest.parent.mkdir(parents=True)
    first.manifest.write_bytes(b"untouched old baseline checkpoint")
    pipeline.source.write_bytes(
        pipeline.source.read_bytes().replace(b"Prior", b"Changed")
    )
    second = main._checkpoint_paths(pipeline.config())
    assert first.manifest.parent != second.manifest.parent
    assert (
        second.manifest.parent.name
        == hashlib.sha256(pipeline.source.read_bytes()).hexdigest()
    )
    main.main()
    assert pipeline.fetched == [2, 10]
    assert first.manifest.read_bytes() == b"untouched old baseline checkpoint"


@pytest.mark.parametrize("old_frame", [False, True])
def test_old_checkpoint_data_rejected(pipeline: Pipeline, old_frame: bool) -> None:
    paths = main._checkpoint_paths(pipeline.config())
    empty = pd.DataFrame(columns=columns).astype("string")
    frame = (
        pd.DataFrame(
            [dict.fromkeys(columns, "") | {"ID": "2"}], columns=columns
        ).astype("string")
        if old_frame
        else empty
    )
    old = InstanceCheckpoint(frame, frozenset({2}))
    snapshot = CheckpointSnapshot((2, 10), InstanceCheckpoint(empty, frozenset()), old)
    save_checkpoint(paths, snapshot)
    source_bytes = pipeline.source.read_bytes()
    with pytest.raises(ValueError, match="must not contain old"):
        main.main()
    assert not pipeline.fetched
    assert paths.manifest.exists()
    assert pipeline.source.read_bytes() == source_bytes
    assert not pipeline.destination.exists()
