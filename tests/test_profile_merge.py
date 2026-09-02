from __future__ import annotations

import pandas as pd
import pytest

from app.constants import (
    COL_COURSES,
    COL_ID,
    COL_MAIL,
    COL_NAME,
    COURSES_COUNT,
    base_urls,
)
from app.profile_merge import MERGED_COLUMNS, merge_profiles

EXPECTED_MERGED_COLUMNS = (
    "ID_old",
    "ID_new",
    "Name",
    "Mail",
    "Courses",
    "Courses Count",
    "Profile_old",
    "Profile_new",
    "AIM_new",
    "AIM_old",
    "Avatar_new",
    "Avatar_old",
    "City_new",
    "City_old",
    "Country_new",
    "Country_old",
    "Description_new",
    "Description_old",
    "ICQ_new",
    "ICQ_old",
    "Images_new",
    "Images_old",
    "Interests_new",
    "Interests_old",
    "Last Access_new",
    "Last Access_old",
    "MSN_new",
    "MSN_old",
    "MoodleNet_new",
    "MoodleNet_old",
    "Skype_new",
    "Skype_old",
    "Timezone_new",
    "Timezone_old",
    "Web_new",
    "Web_old",
    "Yahoo_new",
    "Yahoo_old",
)


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


@pytest.mark.parametrize(
    ("old_email", "new_email"),
    [
        ("\u017f@example.com", "s@example.com"),
        ("ß@example.com", "ss@example.com"),
        (f"{'a' * 65}@example.com", f"{'a' * 65}@example.com"),
    ],
)
def test_unsafe_email_normalization_does_not_match_profiles(
    old_email: str,
    new_email: str,
) -> None:
    # Given addresses that would collide after unsafe normalization.
    old = _profiles([[1, "Old", old_email, ""]])
    new = _profiles([[2, "New", new_email, ""]])

    # When the instance profiles are merged.
    merged = merge_profiles(old, new)

    # Then invalid identity keys leave the profiles separate.
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


def test_empty_raw_frames_use_exact_production_columns() -> None:
    # Given raw frames with no rows or columns.
    old = pd.DataFrame()
    new = pd.DataFrame()

    # When the empty instance profiles are merged.
    merged = merge_profiles(old, new)

    # Then input shape cannot alter the production schema.
    assert MERGED_COLUMNS == EXPECTED_MERGED_COLUMNS
    assert tuple(merged.columns) == EXPECTED_MERGED_COLUMNS
    assert merged.empty


def test_input_columns_cannot_add_or_remove_export_columns() -> None:
    # Given one side with an extra field and another missing a parsed field.
    old = _profiles([[1, "Old", "old@example.com", "Old Course"]]).assign(
        Unexpected="discarded",
    )
    new = pd.DataFrame(
        [[2, "New", "new@example.com"]],
        columns=[COL_ID, COL_NAME, COL_MAIL],
    )

    # When the frames are merged.
    merged = merge_profiles(old, new)

    # Then the output remains exactly the fixed production schema.
    assert tuple(merged.columns) == EXPECTED_MERGED_COLUMNS
    assert "Unexpected_old" not in merged.columns


def test_new_nonblank_identity_fields_win_with_old_fallback() -> None:
    # Given matched rows where new values are alternately populated and blank.
    old = _profiles(
        [
            [1, "Old One", "one@example.com", ""],
            [2, "Old Two", "two@example.com", ""],
            [3, "Old Only", "old-only@example.com", ""],
        ],
    )
    new = _profiles(
        [
            [11, " New One ", " ONE@example.com ", ""],
            [12, "   ", " TWO@example.com ", ""],
        ],
    )

    # When matched profiles are merged.
    merged = merge_profiles(old, new)

    # Then trimmed new values take precedence and blanks fall back to old values.
    assert merged[COL_NAME].tolist() == ["New One", "Old Two", "Old Only"]
    assert merged[COL_MAIL].tolist() == [
        "ONE@example.com",
        "TWO@example.com",
        "old-only@example.com",
    ]
    assert "Name_old" not in merged.columns
    assert "Name_new" not in merged.columns
    assert "Mail_old" not in merged.columns
    assert "Mail_new" not in merged.columns


def test_courses_are_new_first_trimmed_deduplicated_and_counted() -> None:
    # Given matched profiles with overlapping, padded, and blank course lines.
    old = _profiles(
        [[1, "Old", "person@example.com", " Shared Course \nOld Course\n\n"]],
    )
    new = _profiles(
        [[2, "New", "person@example.com", " New Course \nShared Course\nNew Course"]],
    )

    # When matched profiles are merged.
    merged = merge_profiles(old, new)

    # Then first occurrence order is new-before-old and count reflects the union.
    assert merged.loc[0, COL_COURSES] == "New Course\nShared Course\nOld Course"
    assert merged.loc[0, COURSES_COUNT] == 3
