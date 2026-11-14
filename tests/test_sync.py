from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from requests.adapters import HTTPAdapter

from app.historical import HistoricalDataError
from app.profile_merge import MERGED_COLUMNS
from app.sync import (
    BACKUP_PREFIX,
    EXPORT_PREFIX,
    DirectusClient,
    RemoteFile,
    Scrape,
    SyncConfig,
    SyncError,
    run_sync,
)

CONFIG = SyncConfig(url="http://directus:8055", token="token", id_margin=50, keep=2)  # noqa: S106


def _row(old: str, new: str, name: str) -> dict[str, str]:
    return dict.fromkeys(MERGED_COLUMNS, "") | {
        "ID_old": old,
        "ID_new": new,
        "Name": name,
    }


def _csv(rows: list[dict[str, str]]) -> bytes:
    frame = pd.DataFrame(rows, columns=list(MERGED_COLUMNS))
    return frame.to_csv(index=False).encode()


HISTORY = [_row("1", "1", "A"), _row("2", "", "B"), _row("", "30", "C")]


@dataclass
class FakeDirectus:
    files: dict[str, tuple[str, bytes]] = field(default_factory=dict)
    imported: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)

    def list_files(self, prefix: str) -> list[RemoteFile]:
        # Insertion order is upload order; newest first.
        return [
            RemoteFile(file_id, name)
            for file_id, (name, _) in reversed(self.files.items())
            if name.startswith(prefix)
        ]

    def download(self, file_id: str, destination: Path) -> None:
        destination.write_bytes(self.files[file_id][1])

    def upload(self, source: Path, filename: str) -> str:
        file_id = f"f{len(self.files)}"
        self.files[file_id] = (filename, source.read_bytes())
        return file_id

    def run_import(self, file_id: str) -> Mapping[str, Any]:
        self.imported.append(file_id)
        self.files[f"b{len(self.files)}"] = (f"{BACKUP_PREFIX}x.csv", b"")
        return {"rows": 4}

    def delete_files(self, file_ids: Sequence[str]) -> None:
        self.deleted.extend(file_ids)
        for file_id in file_ids:
            del self.files[file_id]


def _now() -> datetime:
    return datetime(2026, 11, 15, 3, 30, tzinfo=UTC)


def _scraper(rows: list[dict[str, str]], calls: list[int] | None = None) -> Scrape:
    def scrape(historical: Path, max_id: int, output: Path, threads: int) -> None:
        assert historical.exists()
        assert threads == CONFIG.threads
        if calls is not None:
            calls.append(max_id)
        output.write_bytes(_csv(rows))

    return scrape


def _seeded() -> FakeDirectus:
    directus = FakeDirectus()
    directus.files["seed"] = (f"{EXPORT_PREFIX}seed.csv", _csv(HISTORY))
    return directus


def test_refreshes_imports_and_prunes(tmp_path: Path) -> None:
    directus = _seeded()
    for index in range(3):
        directus.files[f"old-backup-{index}"] = (f"{BACKUP_PREFIX}{index}.csv", b"")
    calls: list[int] = []
    refreshed = [*HISTORY, _row("", "31", "D")]

    summary = run_sync(CONFIG, directus, _scraper(refreshed, calls), tmp_path, _now)

    assert summary == {"rows": 4}
    assert calls == [80]
    (uploaded,) = directus.imported
    assert directus.files[uploaded][0] == f"{EXPORT_PREFIX}20261115T033000Z.csv"
    assert len(directus.list_files(BACKUP_PREFIX)) == CONFIG.keep
    assert sorted(directus.deleted) == ["old-backup-0", "old-backup-1"]
    assert list(tmp_path.iterdir()) == []


def test_keeps_the_new_export_beyond_the_limit(tmp_path: Path) -> None:
    directus = _seeded()
    directus.files["later"] = (f"{EXPORT_PREFIX}later.csv", _csv(HISTORY))
    directus.files["latest"] = (f"{EXPORT_PREFIX}latest.csv", _csv(HISTORY))

    run_sync(CONFIG, directus, _scraper(HISTORY), tmp_path, _now)

    names = [file.filename for file in directus.list_files(EXPORT_PREFIX)]
    assert names == [
        f"{EXPORT_PREFIX}20261115T033000Z.csv",
        f"{EXPORT_PREFIX}latest.csv",
    ]


@pytest.mark.parametrize(
    "rows",
    [
        pytest.param(HISTORY[:2], id="lost-new-id"),
        pytest.param([HISTORY[0], HISTORY[2]], id="lost-old-id"),
    ],
)
def test_refuses_an_export_that_lost_profiles(
    tmp_path: Path, rows: list[dict[str, str]]
) -> None:
    directus = _seeded()
    scrape = _scraper(rows)

    with pytest.raises(SyncError, match="lost 1 historical"):
        run_sync(CONFIG, directus, scrape, tmp_path, _now)

    assert directus.imported == []
    assert list(directus.files) == ["seed"]
    assert list(tmp_path.iterdir()) == []


def test_refuses_a_malformed_export(tmp_path: Path) -> None:
    directus = _seeded()

    def scrape(historical: Path, max_id: int, output: Path, threads: int) -> None:
        del historical, max_id, threads
        output.write_text("Name\nA\n", encoding="utf-8")

    with pytest.raises(HistoricalDataError):
        run_sync(CONFIG, directus, scrape, tmp_path, _now)
    assert directus.imported == []


def test_requires_a_seed_export(tmp_path: Path) -> None:
    directus = FakeDirectus()
    scrape = _scraper(HISTORY)

    with pytest.raises(SyncError, match="seed"):
        run_sync(CONFIG, directus, scrape, tmp_path, _now)


def test_scraper_failure_stops_before_upload(tmp_path: Path) -> None:
    directus = _seeded()

    def scrape(historical: Path, max_id: int, output: Path, threads: int) -> None:
        del historical, max_id, output, threads
        raise SyncError("Scraper exited with code 2")

    with pytest.raises(SyncError, match="code 2"):
        run_sync(CONFIG, directus, scrape, tmp_path, _now)
    assert list(directus.files) == ["seed"]


def test_config_from_env() -> None:
    config = SyncConfig.from_env(
        {
            "FINKI_HUB_DATA_URL": "http://directus:8055/",
            "FINKI_HUB_DATA_TOKEN": "token",
            "SYNC_ID_MARGIN": "100",
        },
    )
    assert config.url == "http://directus:8055"
    assert config.id_margin == 100
    assert config.keep == 8

    with pytest.raises(SyncError, match="required"):
        SyncConfig.from_env({"FINKI_HUB_DATA_URL": "http://directus:8055"})
    with pytest.raises(SyncError, match="SYNC_KEEP"):
        SyncConfig.from_env(
            {
                "FINKI_HUB_DATA_URL": "http://directus:8055",
                "FINKI_HUB_DATA_TOKEN": "token",
                "SYNC_KEEP": "0",
            },
        )


def test_client_waits_out_a_directus_restart() -> None:
    client = DirectusClient(CONFIG)
    adapter = client._session.get_adapter(f"{CONFIG.url}/files")
    assert isinstance(adapter, HTTPAdapter)
    retry = adapter.max_retries

    assert retry.connect == 8
    assert retry.status_forcelist is not None
    assert 503 in retry.status_forcelist
    # Never resend an upload or an import after the request went out.
    assert retry.allowed_methods is not None
    assert "POST" not in retry.allowed_methods
