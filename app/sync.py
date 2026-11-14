"""Unattended new-instance refresh of the FINKI Hub data (Directus) snapshot.

1. Download the newest ``profiles-*.csv`` from the private "Moodle snapshots"
   folder as the historical export.
2. Scrape the new instance only, up to the highest known ID plus a margin.
3. Check that the result still holds every historical profile.
4. Upload it to the same folder and run the "Data / Import Moodle snapshot"
   Flow, which backs up and replaces the active snapshot.
5. Keep only the newest exports and import backups in the folder.

Logs carry counts, file ids and error codes only, never profile contents.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, Protocol

import requests

from app.cli import OUTPUT_DIRECTORY
from app.historical import (
    HistoricalDataError,
    HistoricalSnapshot,
    load_historical_snapshot,
)

__all__ = [
    "SyncConfig",
    "SyncError",
    "main",
    "run_sync",
]

logger = logging.getLogger(__name__)

# Declared by the finki-hub/data schema (src/flows/snapshot-folder.ts and
# src/flows/snapshot-import.ts); stable across installs.
SNAPSHOT_FOLDER_ID: Final = "6a1d9f0e-2b3c-4d5e-8f70-0000000010f0"
IMPORT_FLOW_ID: Final = "6a1d9f0e-2b3c-4d5e-8f70-000000001001"
IMPORT_FLOW_COLLECTION: Final = "data_operations"

EXPORT_PREFIX: Final = "profiles-"
BACKUP_PREFIX: Final = "moodle-users-backup-"
HISTORICAL_FILE: Final = "historical.csv"

_REQUEST_TIMEOUT: Final = 120.0
# The import is synchronous and bounded by Directus' IMPORT_TIMEOUT.
_IMPORT_TIMEOUT: Final = 3600.0
_CHUNK: Final = 1 << 20


class SyncError(Exception):
    """A step failed; the message never contains profile data."""


@dataclass(frozen=True, slots=True)
class SyncConfig:
    url: str
    token: str
    id_margin: int = 2000
    keep: int = 8
    threads: int = 10

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> SyncConfig:
        url = env.get("FINKI_HUB_DATA_URL", "").strip().rstrip("/")
        token = env.get("FINKI_HUB_DATA_TOKEN", "").strip()
        if not url or not token:
            raise SyncError("FINKI_HUB_DATA_URL and FINKI_HUB_DATA_TOKEN are required")
        return cls(
            url=url,
            token=token,
            id_margin=_positive(env, "SYNC_ID_MARGIN", 2000),
            keep=_positive(env, "SYNC_KEEP", 8),
            threads=_positive(env, "SYNC_THREADS", 10),
        )


def _positive(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise SyncError(f"{name} must be a positive integer") from None
    if value <= 0:
        raise SyncError(f"{name} must be a positive integer")
    return value


@dataclass(frozen=True, slots=True)
class RemoteFile:
    id: str
    filename: str


class Directus(Protocol):
    def list_files(self, prefix: str) -> list[RemoteFile]:
        """Folder files whose name starts with prefix, newest first."""
        ...

    def download(self, file_id: str, destination: Path) -> None: ...

    def upload(self, source: Path, filename: str) -> str: ...

    def run_import(self, file_id: str) -> Mapping[str, Any]: ...

    def delete_files(self, file_ids: Sequence[str]) -> None: ...


class DirectusClient:
    def __init__(self, config: SyncConfig) -> None:
        self._url = config.url
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {config.token}"

    def _check(self, response: requests.Response, action: str) -> None:
        if response.ok:
            return
        raise SyncError(
            f"{action} failed: HTTP {response.status_code} {_errors(response)}"
        )

    def list_files(self, prefix: str) -> list[RemoteFile]:
        response = self._session.get(
            f"{self._url}/files",
            params={
                "filter[folder][_eq]": SNAPSHOT_FOLDER_ID,
                "filter[filename_download][_starts_with]": prefix,
                "sort": "-uploaded_on",
                "fields": "id,filename_download",
                "limit": "-1",
            },
            timeout=_REQUEST_TIMEOUT,
        )
        self._check(response, "Listing snapshot files")
        return [
            RemoteFile(item["id"], item["filename_download"])
            for item in response.json()["data"]
        ]

    def download(self, file_id: str, destination: Path) -> None:
        with self._session.get(
            f"{self._url}/assets/{file_id}",
            params={"download": ""},
            stream=True,
            timeout=_REQUEST_TIMEOUT,
        ) as response:
            self._check(response, "Downloading the historical export")
            with destination.open("wb") as handle:
                for chunk in response.iter_content(_CHUNK):
                    handle.write(chunk)

    def upload(self, source: Path, filename: str) -> str:
        with source.open("rb") as handle:
            response = self._session.post(
                f"{self._url}/files",
                # Directus reads the fields before the file part.
                data={"folder": SNAPSHOT_FOLDER_ID, "title": filename},
                files={"file": (filename, handle, "text/csv")},
                timeout=_IMPORT_TIMEOUT,
            )
        self._check(response, "Uploading the export")
        return str(response.json()["data"]["id"])

    def run_import(self, file_id: str) -> Mapping[str, Any]:
        response = self._session.post(
            f"{self._url}/flows/trigger/{IMPORT_FLOW_ID}",
            json={
                "collection": IMPORT_FLOW_COLLECTION,
                "file": file_id,
                "confirm_replace": True,
            },
            timeout=_IMPORT_TIMEOUT,
        )
        self._check(response, "Import Moodle snapshot Flow")
        body = response.json() if response.content else {}
        summary = body.get("data", body) if isinstance(body, dict) else {}
        return summary if isinstance(summary, dict) else {}

    def delete_files(self, file_ids: Sequence[str]) -> None:
        if not file_ids:
            return
        response = self._session.delete(
            f"{self._url}/files",
            json=list(file_ids),
            timeout=_REQUEST_TIMEOUT,
        )
        self._check(response, "Removing old snapshot files")


def _errors(response: requests.Response) -> str:
    """The Directus error codes and messages, bounded."""
    try:
        errors = response.json().get("errors", [])
        text = "; ".join(
            f"{error.get('extensions', {}).get('code', '')} {error.get('message', '')}"
            for error in errors
        )
    except (ValueError, AttributeError):
        text = ""
    return text.strip()[:500]


type Scrape = Callable[[Path, int, Path, int], None]


def scrape_new_instance(
    historical: Path, max_id: int, output: Path, threads: int
) -> None:
    """Run the scraper CLI in new-only CAS mode (credentials from the env)."""
    command = [
        sys.executable,
        "-m",
        "app",
        "--instances",
        "new",
        "--historical",
        str(historical),
        "--cas",
        "-m",
        str(max_id),
        "-t",
        str(threads),
        "-o",
        output.name,
    ]
    completed = subprocess.run(command, check=False)  # noqa: S603
    if completed.returncode != 0:
        raise SyncError(f"Scraper exited with code {completed.returncode}")


def _ids(snapshot: HistoricalSnapshot, column: str) -> set[str]:
    return {value for value in snapshot.frame[column] if value}


def max_new_id(snapshot: HistoricalSnapshot) -> int:
    ids = _ids(snapshot, "ID_new")
    if not ids:
        raise SyncError("The historical export has no new-instance IDs")
    return max(int(value) for value in ids)


def check_export(historical: HistoricalSnapshot, export: HistoricalSnapshot) -> None:
    """The refreshed export must keep every historical profile."""
    for column in ("ID_old", "ID_new"):
        missing = _ids(historical, column) - _ids(export, column)
        if missing:
            raise SyncError(
                f"The export lost {len(missing)} historical {column} values"
            )
    if len(export.frame) < len(historical.frame):
        raise SyncError(
            f"The export has {len(export.frame)} rows, fewer than the "
            f"historical {len(historical.frame)}",
        )


def _stale(
    files: Sequence[RemoteFile], keep: int, current: str | None = None
) -> list[str]:
    kept = [file.id for file in files[:keep]]
    if current is not None and current not in kept:
        kept.append(current)
    return [file.id for file in files if file.id not in kept]


def run_sync(
    config: SyncConfig,
    directus: Directus,
    scrape: Scrape = scrape_new_instance,
    work: Path = OUTPUT_DIRECTORY,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Mapping[str, Any]:
    work.mkdir(parents=True, exist_ok=True)
    exports = directus.list_files(EXPORT_PREFIX)
    if not exports:
        raise SyncError(
            f'No {EXPORT_PREFIX}*.csv in "Moodle snapshots (private)": upload the '
            "latest combined export there once to seed the refresh",
        )
    source = exports[0]
    historical_path = work / HISTORICAL_FILE
    output = work / f"{EXPORT_PREFIX}{now():%Y%m%dT%H%M%SZ}.csv"
    try:
        return _refresh(config, directus, scrape, _Run(source, historical_path, output))
    finally:
        # Profile data lives in Directus; only resumable checkpoints stay.
        historical_path.unlink(missing_ok=True)
        output.unlink(missing_ok=True)


@dataclass(frozen=True, slots=True)
class _Run:
    source: RemoteFile
    historical: Path
    output: Path


def _refresh(
    config: SyncConfig,
    directus: Directus,
    scrape: Scrape,
    run: _Run,
) -> Mapping[str, Any]:
    logger.info("Historical export: %s (%s)", run.source.filename, run.source.id)
    directus.download(run.source.id, run.historical)
    historical = load_historical_snapshot(run.historical)
    max_id = max_new_id(historical) + config.id_margin
    logger.info(
        "Historical rows: %d, scraping new-instance IDs 1-%d",
        len(historical.frame),
        max_id,
    )

    filename = run.output.name
    scrape(run.historical, max_id, run.output, config.threads)
    export = load_historical_snapshot(run.output)
    check_export(historical, export)
    logger.info(
        "Export rows: %d (%+d), new-instance profiles: %d",
        len(export.frame),
        len(export.frame) - len(historical.frame),
        len(_ids(export, "ID_new")),
    )

    file_id = directus.upload(run.output, filename)
    logger.info("Uploaded %s (%s), importing", filename, file_id)
    summary = directus.run_import(file_id)
    logger.info(
        "Imported: rows %s, previous %s, backup %s, warnings %s",
        summary.get("rows"),
        summary.get("previousRows"),
        summary.get("backup"),
        summary.get("warnings"),
    )

    stale = _stale(directus.list_files(EXPORT_PREFIX), config.keep, file_id)
    stale += _stale(directus.list_files(BACKUP_PREFIX), config.keep)
    directus.delete_files(stale)
    logger.info("Removed %d old snapshot files", len(stale))
    return summary


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    try:
        config = SyncConfig.from_env(os.environ)
        run_sync(config, DirectusClient(config))
    except (SyncError, HistoricalDataError) as error:
        logger.error("Sync failed: %s", error)  # noqa: TRY400
        raise SystemExit(1) from None
    except requests.RequestException as error:
        logger.error("Sync failed: %s", type(error).__name__)  # noqa: TRY400
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
