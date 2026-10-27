"""Read-only historical merged snapshots and ID-based incremental refreshes."""

import csv
import hashlib
import io
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from app.constants import (
    COL_COURSES,
    COL_MAIL,
    COL_NAME,
    COURSES_COUNT,
    FIRST_UNMIGRATED_OLD_ID,
    columns,
)
from app.profile_merge import MERGED_COLUMNS, merge_profiles

__all__ = [
    "HistoricalDataError",
    "HistoricalSnapshot",
    "load_historical_snapshot",
    "merge_historical_snapshot",
]


class HistoricalDataError(Exception):
    """A snapshot cannot safely be used; never expose its contents."""

    def __str__(self) -> str:
        return "historical snapshot is unreadable or invalid"


@dataclass(frozen=True)
class HistoricalSnapshot:
    frame: pd.DataFrame
    path: Path
    sha256: str


def _normalize_side_id(value: str, side: str, seen: set[str]) -> str:
    if not value:
        return ""
    if not re.fullmatch(r"\d+", value, flags=re.ASCII) or int(value) <= 0:
        raise HistoricalDataError
    value = str(int(value))
    if value in seen:
        raise HistoricalDataError
    if side == "ID_old" and int(value) >= FIRST_UNMIGRATED_OLD_ID:
        raise HistoricalDataError
    seen.add(value)
    return value


def _normalize_ids(rows: list[dict[str, str]]) -> None:
    seen: dict[str, set[str]] = {"ID_old": set(), "ID_new": set()}
    identities: set[str] = set()
    for row in rows:
        for side, values in seen.items():
            row[side] = _normalize_side_id(row[side], side, values)
        old, new = row["ID_old"], row["ID_new"]
        if (not old and not new) or (old and new and old != new):
            raise HistoricalDataError
        identity = new or old
        if identity in identities:
            raise HistoricalDataError
        identities.add(identity)


def load_historical_snapshot(path: Path) -> HistoricalSnapshot:
    """Load and hash the same bytes, without NA inference or source writes."""
    try:
        resolved = path.resolve()
        raw = resolved.read_bytes()
        previous_limit = csv.field_size_limit()
        try:
            # No decoded field can exceed the size of the already-loaded bytes.
            csv.field_size_limit(max(previous_limit, len(raw)))
            reader = csv.reader(
                io.StringIO(raw.decode("utf-8-sig"), newline=""), strict=True
            )
            if next(reader, None) != list(MERGED_COLUMNS):
                raise HistoricalDataError
            rows = []
            for values in reader:
                if len(values) != len(MERGED_COLUMNS):
                    raise HistoricalDataError
                rows.append(dict(zip(MERGED_COLUMNS, values, strict=True)))
        finally:
            csv.field_size_limit(previous_limit)
        if not rows:
            raise HistoricalDataError
        _normalize_ids(rows)
        frame = pd.DataFrame(rows, columns=list(MERGED_COLUMNS), dtype=str)
        return HistoricalSnapshot(frame, resolved, hashlib.sha256(raw).hexdigest())
    except (OSError, csv.Error, ValueError):
        raise HistoricalDataError from None


def _records(frame: pd.DataFrame) -> list[dict[str, str]]:
    return [
        dict(zip(MERGED_COLUMNS, values, strict=True))
        for values in frame.fillna("").astype(str).itertuples(index=False, name=None)
    ]


def _courses(fresh: str, historical: str) -> str:
    return "\n".join(
        dict.fromkeys(
            course.strip()
            for raw in (fresh, historical)
            for course in raw.split("\n")
            if course.strip()
        ),
    )


def _refresh_row(historical: dict[str, str], current: dict[str, str]) -> None:
    for field in MERGED_COLUMNS:
        if field.endswith("_new"):
            historical[field] = current[field]
    for field in (COL_NAME, COL_MAIL):
        if current[field].strip():
            historical[field] = current[field]
    historical[COL_COURSES] = _courses(
        current[COL_COURSES],
        historical[COL_COURSES],
    )


def merge_historical_snapshot(
    snapshot: HistoricalSnapshot,
    df_new: pd.DataFrame,
) -> pd.DataFrame:
    """Refresh successful new profiles while retaining every historical row."""
    fresh = _records(merge_profiles(pd.DataFrame(columns=columns), df_new))
    rows = _records(snapshot.frame)
    by_new = {row["ID_new"]: row for row in rows if row["ID_new"]}
    old_only = {
        row["ID_old"]: row for row in rows if row["ID_old"] and not row["ID_new"]
    }
    for current in fresh:
        identity = current["ID_new"]
        historical = by_new.get(identity)
        if historical is None and int(identity) < FIRST_UNMIGRATED_OLD_ID:
            historical = old_only.get(identity)
        if historical is None:
            rows.append(current)
            continue
        _refresh_row(historical, current)
    for row in rows:
        row[COURSES_COUNT] = str(len(row[COL_COURSES].splitlines()))
    rows.sort(
        key=lambda row: (
            not bool(row["ID_new"]),
            int(row["ID_new"] or "0"),
            not bool(row["ID_old"]),
            int(row["ID_old"] or "0"),
        ),
    )
    return pd.DataFrame(rows, columns=list(MERGED_COLUMNS))
