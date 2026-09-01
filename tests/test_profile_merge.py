from __future__ import annotations

import pandas as pd

from app.constants import COL_COURSES, COL_ID, COL_MAIL, COL_NAME, base_urls
from app.profile_merge import merge_profiles


def _profiles(rows: list[list[str | int]]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=[COL_ID, COL_NAME, COL_MAIL, COL_COURSES],
    )


def test_unique_normalized_email_matches_instance_profiles() -> None:
    # Given one account with different local IDs and normalized email spelling.
    old = _profiles([[11, "Old Name", " person@example.com ", "Old Course"]])
    new = _profiles([[22, "New Name", "PERSON@example.com", "New Course"]])

    # When the instance profiles are merged.
    merged = merge_profiles(old, new)

    # Then both local IDs and their corresponding URLs share one row.
    assert len(merged) == 1
    assert merged.loc[0, "ID_old"] == "11"
    assert merged.loc[0, "ID_new"] == "22"
    assert merged.loc[0, "Profile_old"] == f"{base_urls['old']}/user/profile.php?id=11"
    assert merged.loc[0, "Profile_new"] == f"{base_urls['new']}/user/profile.php?id=22"


def test_duplicated_email_does_not_match_profiles() -> None:
    # Given an email duplicated on one instance.
    old = _profiles(
        [
            [1, "Old One", "person@example.com", ""],
            [2, "Old Two", "person@example.com", ""],
        ],
    )
    new = _profiles([[3, "New", "person@example.com", ""]])

    # When the instance profiles are merged.
    merged = merge_profiles(old, new)

    # Then ambiguity keeps all source profiles separate.
    assert len(merged) == 3
    assert not ((merged["ID_old"] != "") & (merged["ID_new"] != "")).any()


def test_malformed_email_does_not_match_profiles() -> None:
    # Given two profiles with the same invalid dot-atom email.
    columns = [COL_ID, COL_NAME, COL_MAIL, COL_COURSES]
    old = pd.DataFrame([[1, "Old", ".bad@example.com", ""]], columns=columns)
    new = pd.DataFrame([[2, "New", ".bad@example.com", ""]], columns=columns)

    # When the instance profiles are merged.
    merged = merge_profiles(old, new)

    # Then the malformed email does not correlate the profiles.
    assert len(merged) == 2


def test_profile_ids_sort_numerically() -> None:
    # Given IDs whose lexical and numeric orders differ.
    old = _profiles([])
    new = _profiles(
        [
            [10, "Ten", "ten@example.com", ""],
            [2, "Two", "two@example.com", ""],
        ],
    )

    # When the instance profiles are merged.
    merged = merge_profiles(old, new)

    # Then local IDs use numeric order.
    assert merged["ID_new"].tolist() == ["2", "10"]
