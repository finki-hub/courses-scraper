from typing import Final, Literal

import pandas as pd

from app.constants import (
    COL_COURSES,
    COL_ID,
    COL_MAIL,
    COL_NAME,
    COL_PROFILE,
    COURSES_COUNT,
    base_urls,
)

__all__ = ["merge_profiles"]

_EMAIL_KEY = "_email_key"
_ID_OLD = f"{COL_ID}_old"
_ID_NEW = f"{COL_ID}_new"
_PROFILE_OLD = f"{COL_PROFILE}_old"
_PROFILE_NEW = f"{COL_PROFILE}_new"
_EMAIL_PATTERN: Final = (
    r"[a-z0-9!#$%&'*+/=?^_`{|}~-]+"
    r"(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*"
    r"@(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}"
)


def _series(df: pd.DataFrame, column: str) -> pd.Series:
    result = df[column]
    if not isinstance(result, pd.Series):
        raise TypeError(f"Expected one column for {column}")
    return result


def _dataframe(value: object) -> pd.DataFrame:
    if not isinstance(value, pd.DataFrame):
        raise TypeError("Expected a DataFrame")
    return value


def _normalize_side(
    df: pd.DataFrame,
    side: Literal["old", "new"],
) -> pd.DataFrame:
    ids = pd.to_numeric(_series(df, COL_ID), errors="raise")
    if not isinstance(ids, pd.Series):
        raise TypeError("Expected profile IDs to remain a Series")
    ids = ids.astype("Int64")
    if ids.isna().any() or (ids <= 0).any() or ids.duplicated().any():
        raise pd.errors.MergeError(f"{side} profile IDs must be positive and unique")

    normalized = df.rename(columns={column: f"{column}_{side}" for column in df})
    normalized[f"{COL_ID}_{side}"] = ids
    emails = normalized[f"{COL_MAIL}_{side}"].fillna("").astype(str).str.strip()
    normalized_emails = emails.str.lower()
    local_part_lengths = emails.str.split("@", n=1).str[0].str.len()
    valid_emails = (
        emails.str.fullmatch(r"[\x00-\x7f]+", na=False)
        & (emails.str.len() <= 254)
        & (local_part_lengths <= 64)
        & normalized_emails.str.fullmatch(_EMAIL_PATTERN, na=False)
    )
    normalized[_EMAIL_KEY] = normalized_emails.where(valid_emails, "")
    return normalized


def _unique_shared_emails(
    old: pd.DataFrame,
    new: pd.DataFrame,
) -> list[str]:
    old_counts = _series(old, _EMAIL_KEY).value_counts()
    new_counts = _series(new, _EMAIL_KEY).value_counts()
    unique_old = {
        str(email) for email, count in old_counts.items() if email and count == 1
    }
    unique_new = {
        str(email) for email, count in new_counts.items() if email and count == 1
    }
    return sorted(unique_old & unique_new)


def _coalesce_field(merged: pd.DataFrame, field: str) -> None:
    old = _series(merged, f"{field}_old").fillna("").astype(str).str.strip()
    new = _series(merged, f"{field}_new").fillna("").astype(str).str.strip()
    merged[field] = new.where(new != "", old)


def _merge_courses(old: str, new: str) -> str:
    combined = [
        course.strip()
        for raw in (new, old)
        for course in raw.split("\n")
        if course.strip()
    ]
    return "\n".join(dict.fromkeys(combined))


def _add_profile_urls(merged: pd.DataFrame) -> None:
    for side, id_column, profile_column in (
        ("old", _ID_OLD, _PROFILE_OLD),
        ("new", _ID_NEW, _PROFILE_NEW),
    ):
        ids = _series(merged, id_column).astype("Int64").astype("string").fillna("")
        merged[id_column] = ids
        merged[profile_column] = (
            base_urls[side] + "/user/profile.php?id=" + ids
        ).where(
            ids != "",
            "",
        )


def _column_order(merged: pd.DataFrame) -> list[str]:
    excluded = {
        _EMAIL_KEY,
        _ID_OLD,
        _ID_NEW,
        _PROFILE_OLD,
        _PROFILE_NEW,
        COL_NAME,
        COL_MAIL,
        COL_COURSES,
        COURSES_COUNT,
        f"{COL_NAME}_old",
        f"{COL_NAME}_new",
        f"{COL_MAIL}_old",
        f"{COL_MAIL}_new",
        f"{COL_COURSES}_old",
        f"{COL_COURSES}_new",
    }
    remaining: list[str] = sorted(
        str(column) for column in merged if column not in excluded
    )
    return [
        _ID_OLD,
        _ID_NEW,
        COL_NAME,
        COL_MAIL,
        COL_COURSES,
        COURSES_COUNT,
        _PROFILE_OLD,
        _PROFILE_NEW,
        *remaining,
    ]


def merge_profiles(df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    old = _normalize_side(df_old, "old")
    new = _normalize_side(df_new, "new")
    shared_emails = _unique_shared_emails(old, new)

    matched = old[_series(old, _EMAIL_KEY).isin(shared_emails)].merge(
        new[_series(new, _EMAIL_KEY).isin(shared_emails)],
        on=_EMAIL_KEY,
        how="inner",
        validate="one_to_one",
    )
    merged = _dataframe(
        pd.concat(
            [
                matched,
                old[~_series(old, _EMAIL_KEY).isin(shared_emails)],
                new[~_series(new, _EMAIL_KEY).isin(shared_emails)],
            ],
            ignore_index=True,
            sort=False,
        ),
    )

    for field in (COL_NAME, COL_MAIL):
        _coalesce_field(merged, field)

    old_courses = _series(merged, f"{COL_COURSES}_old").fillna("").astype(str)
    new_courses = _series(merged, f"{COL_COURSES}_new").fillna("").astype(str)
    merged[COL_COURSES] = [
        _merge_courses(old_value, new_value)
        for old_value, new_value in zip(old_courses, new_courses, strict=True)
    ]
    merged[COURSES_COUNT] = _series(merged, COL_COURSES).apply(
        lambda courses: len(courses.split("\n")) if courses else 0,
    )
    _add_profile_urls(merged)

    merged["_sort_id_new"] = pd.to_numeric(_series(merged, _ID_NEW), errors="coerce")
    merged["_sort_id_old"] = pd.to_numeric(_series(merged, _ID_OLD), errors="coerce")
    merged = _dataframe(
        merged.sort_values(
            ["_sort_id_new", "_sort_id_old"],
            kind="stable",
            na_position="last",
        ).drop(columns=["_sort_id_new", "_sort_id_old"]),
    )
    return _dataframe(
        merged.loc[:, _column_order(merged)].reset_index(drop=True),
    )
