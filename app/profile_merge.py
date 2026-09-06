from typing import Final, Literal

import pandas as pd

from app.constants import (
    COL_COURSES,
    COL_ID,
    COL_MAIL,
    COL_NAME,
    COL_PROFILE,
    COURSES_COUNT,
    FIRST_UNMIGRATED_OLD_ID,
    base_urls,
    columns,
)

__all__ = ["MERGED_COLUMNS", "merge_profiles"]

_ID_OLD = f"{COL_ID}_old"
_ID_NEW = f"{COL_ID}_new"
_PROFILE_OLD = f"{COL_PROFILE}_old"
_PROFILE_NEW = f"{COL_PROFILE}_new"
MERGED_COLUMNS: Final = (
    _ID_OLD,
    _ID_NEW,
    COL_NAME,
    COL_MAIL,
    COL_COURSES,
    COURSES_COUNT,
    _PROFILE_OLD,
    _PROFILE_NEW,
    *sorted(
        f"{column}_{side}"
        for column in columns
        if column not in {COL_ID, COL_NAME, COL_MAIL, COL_COURSES}
        for side in ("old", "new")
    ),
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
    normalized_input = df.reindex(columns=columns).copy(deep=True)
    ids = pd.to_numeric(_series(normalized_input, COL_ID), errors="raise")
    if not isinstance(ids, pd.Series):
        raise TypeError("Expected profile IDs to remain a Series")
    ids = ids.astype("Int64")
    if ids.isna().any() or (ids <= 0).any() or ids.duplicated().any():
        raise pd.errors.MergeError(f"{side} profile IDs must be positive and unique")

    normalized = normalized_input.rename(
        columns={column: f"{column}_{side}" for column in normalized_input},
    )
    normalized[f"{COL_ID}_{side}"] = ids
    return normalized


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


def merge_profiles(df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    old = _normalize_side(df_old, "old")
    new = _normalize_side(df_new, "new")
    old = old[_series(old, _ID_OLD) < FIRST_UNMIGRATED_OLD_ID]
    merged = old.merge(
        new,
        left_on=_ID_OLD,
        right_on=_ID_NEW,
        how="outer",
        validate="one_to_one",
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
        merged.loc[:, list(MERGED_COLUMNS)].reset_index(drop=True),
    )
