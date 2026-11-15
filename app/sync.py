"""Unattended new-instance refresh of the FINKI Hub data (Directus) snapshot.

1. Download the newest ``profiles-*.csv`` from the private "Moodle snapshots"
   folder as the historical export.
2. Scrape the new instance only, up to the highest known ID plus a margin;
   a failed scrape (CAS or Moodle briefly unreachable) is retried after a
   pause and resumes from its checkpoints.
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
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, Protocol

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.cli import OUTPUT_DIRECTORY
from app.constants import EXIT_REFUSED
from app.historical import (
    HistoricalDataError,
    HistoricalSnapshot,
    load_historical_snapshot,
)

__all__ = [
    "Clock",
    "ScrapeError",
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

# Directus restarts when Watchtower deploys a new image: wait it out (about
# four minutes in all) instead of failing. A refused connection is retried
# for every method, since nothing was sent; a 502/503/504 or a read error
# only for the idempotent reads and deletes, never for an upload or import.
_RETRY: Final = Retry(
    total=8,
    connect=8,
    read=3,
    status=8,
    backoff_factor=2,
    backoff_max=60,
    status_forcelist=(502, 503, 504),
    allowed_methods=frozenset({"GET", "DELETE"}),
    raise_on_status=False,
)


class SyncError(Exception):
    """A step failed; the message never contains profile data."""


class ScrapeError(SyncError):
    """The scraper exited with an error; ``retryable`` unless CAS refused
    the login or the arguments were wrong, which a retry cannot fix (and a
    repeated refused login could lock the account)."""

    def __init__(self, returncode: int) -> None:
        super().__init__(f"Scraper exited with code {returncode}")
        self.retryable = returncode != EXIT_REFUSED


@dataclass(frozen=True, slots=True)
class SyncConfig:
    url: str
    token: str
    id_margin: int = 2000
    keep: int = 8
    threads: int = 10
    attempts: int = 3
    retry_delay: int = 900

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
            attempts=_positive(env, "SYNC_ATTEMPTS", 3),
            retry_delay=_positive(env, "SYNC_RETRY_DELAY", 900, minimum=0),
        )


def _positive(
    env: Mapping[str, str], name: str, default: int, *, minimum: int = 1
) -> int:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    kind = "a positive" if minimum == 1 else "a non-negative"
    try:
        value = int(raw)
    except ValueError:
        raise SyncError(f"{name} must be {kind} integer") from None
    if value < minimum:
        raise SyncError(f"{name} must be {kind} integer")
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
        self._session.mount(config.url, HTTPAdapter(max_retries=_RETRY))

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
    except ValueError, AttributeError:
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
        raise ScrapeError(completed.returncode)


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


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class Clock:
    """The time source of a run and how it waits between scrape attempts."""

    now: Callable[[], datetime] = _utc_now
    sleep: Callable[[float], None] = time.sleep


def run_sync(
    config: SyncConfig,
    directus: Directus,
    scrape: Scrape = scrape_new_instance,
    work: Path = OUTPUT_DIRECTORY,
    clock: Clock | None = None,
) -> Mapping[str, Any]:
    clock = clock or Clock()
    work.mkdir(parents=True, exist_ok=True)
    exports = directus.list_files(EXPORT_PREFIX)
    if not exports:
        raise SyncError(
            f'No {EXPORT_PREFIX}*.csv in "Moodle snapshots (private)": upload the '
            "latest combined export there once to seed the refresh",
        )
    source = exports[0]
    historical_path = work / HISTORICAL_FILE
    output = work / f"{EXPORT_PREFIX}{clock.now():%Y%m%dT%H%M%SZ}.csv"
    try:
        return _refresh(
            config,
            directus,
            scrape,
            _Run(source, historical_path, output, clock.sleep),
        )
    finally:
        # Profile data lives in Directus; only resumable checkpoints stay.
        historical_path.unlink(missing_ok=True)
        output.unlink(missing_ok=True)


@dataclass(frozen=True, slots=True)
class _Run:
    source: RemoteFile
    historical: Path
    output: Path
    sleep: Callable[[float], None]


def _scrape(config: SyncConfig, scrape: Scrape, run: _Run, max_id: int) -> None:
    """Scrape, retrying a retryable failure after ``retry_delay`` seconds."""
    for attempt in range(1, config.attempts + 1):
        try:
            scrape(run.historical, max_id, run.output, config.threads)
        except ScrapeError as error:
            if not error.retryable or attempt == config.attempts:
                raise
            logger.warning(
                "Scrape attempt %d of %d failed (%s); retrying in %d s",
                attempt,
                config.attempts,
                error,
                config.retry_delay,
            )
            run.sleep(config.retry_delay)
        else:
            return


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
    _scrape(config, scrape, run, max_id)
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


def _sync() -> str | None:
    """Run the refresh; the bounded reason it failed, or None."""
    try:
        config = SyncConfig.from_env(os.environ)
        run_sync(config, DirectusClient(config))
    except (SyncError, HistoricalDataError) as error:
        return str(error)
    except requests.RequestException as error:
        # The type only: a request error may quote a URL or a response.
        return type(error).__name__
    return None


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    failure = _sync()
    if failure is not None:
        logger.error("Sync failed: %s", failure)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
