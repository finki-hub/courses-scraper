import pandas as pd
import pytest

from app.constants import COL_COURSES, COL_ID, COL_MAIL, COL_NAME
from app.profile_merge import merge_profiles


@pytest.mark.parametrize(
    ("old_mail", "new_mail"),
    [
        ("", ""),
        ("visible@example.com", ""),
        ("", "visible@example.com"),
        ("before@example.com", "after@example.com"),
        ("malformed", "malformed"),
    ],
)
def test_migrated_id_matches_independently_of_email(
    old_mail: str,
    new_mail: str,
) -> None:
    # Given the same migrated ID with hidden, edited, or invalid emails.
    old = pd.DataFrame(
        [
            {
                COL_ID: 42,
                COL_NAME: "Before",
                COL_MAIL: old_mail,
                COL_COURSES: "Archived course",
            }
        ]
    )
    new = pd.DataFrame(
        [
            {
                COL_ID: 42,
                COL_NAME: "After",
                COL_MAIL: new_mail,
                COL_COURSES: "Current course",
            }
        ]
    )

    # When profiles are merged.
    merged = merge_profiles(old, new)

    # Then the ID identifies one account and preserves both course histories.
    assert len(merged) == 1
    assert merged.loc[0, "ID_old"] == merged.loc[0, "ID_new"] == "42"
    assert merged.loc[0, COL_MAIL] == (new_mail or old_mail)
    assert merged.loc[0, COL_COURSES] == "Current course\nArchived course"


def test_email_cannot_match_different_migrated_ids() -> None:
    # Given different accounts that share an email address.
    old = pd.DataFrame([{COL_ID: 41, COL_MAIL: "shared@example.com"}])
    new = pd.DataFrame([{COL_ID: 42, COL_MAIL: "shared@example.com"}])

    # When profiles are merged.
    merged = merge_profiles(old, new)

    # Then an email cannot override the migrated identity.
    assert len(merged) == 2
    assert not ((merged["ID_old"] != "") & (merged["ID_new"] != "")).any()


@pytest.mark.parametrize("side", ["old", "new"])
@pytest.mark.parametrize("ids", [[1, 1], [0], [-1], [None]])
def test_invalid_source_ids_fail_closed(side: str, ids: list[int | None]) -> None:
    # Given an ambiguous or missing identity on either source.
    invalid = pd.DataFrame({COL_ID: ids})
    old, new = (invalid, pd.DataFrame()) if side == "old" else (pd.DataFrame(), invalid)

    # When the invalid source reaches the merger, then it fails closed.
    with pytest.raises(pd.errors.MergeError):
        merge_profiles(old, new)


def test_old_profiles_are_excluded_from_first_divergent_id() -> None:
    # Given adjacent IDs at the observed boundary and a later archive-only row.
    old = pd.DataFrame(
        [
            {COL_ID: 16338, COL_NAME: "Migrated", COL_COURSES: "Archive"},
            {COL_ID: 16339, COL_NAME: "Unrelated", COL_COURSES: "Wrong history"},
            {COL_ID: 16340, COL_NAME: "Archive only"},
        ]
    )
    new = pd.DataFrame(
        [
            {COL_ID: 16338, COL_NAME: "Migrated"},
            {COL_ID: 16339, COL_NAME: "Current"},
        ]
    )
    before = old.copy(deep=True)

    # When old data, including a pre-existing checkpoint, reaches the merger.
    merged = merge_profiles(old, new)

    # Then the cutoff is exclusive and the current account remains intact.
    assert merged["ID_old"].tolist() == ["16338", ""]
    assert merged["ID_new"].tolist() == ["16338", "16339"]
    assert merged[COL_COURSES].tolist() == ["Archive", ""]
    pd.testing.assert_frame_equal(old, before)
