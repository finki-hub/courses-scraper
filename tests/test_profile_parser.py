from pathlib import Path

from app.constants import selectors_new, selectors_old
from app.profile_parser import parse_profile_html

FIXTURES = Path(__file__).parent / "fixtures"


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_empty_page_returns_no_profile() -> None:
    # Given a successful response with no profile content.
    html = "<html><body><main id='region-main'></main></body></html>"

    # When the response HTML is parsed.
    profile = parse_profile_html(html, selectors_new)

    # Then it is not treated as a Moodle profile.
    assert profile == {}


def test_avatar_only_page_returns_no_profile() -> None:
    # Given a page containing only Moodle's profile-image markup.
    html = (
        "<html><body><div class='page-header-image'>"
        "<img class='userpicture' src='avatar.png'>"
        "</div></body></html>"
    )

    # When the response HTML is parsed.
    profile = parse_profile_html(html, selectors_new)

    # Then an image alone cannot prove that the page is a substantive profile.
    assert profile == {}


def test_new_instance_profile_fixture_parses_stable_fields() -> None:
    # Given representative HTML from the new Moodle instance.
    html = _fixture("profile_new.html")

    # When the profile is parsed with the new-instance selectors.
    profile = parse_profile_html(html, selectors_new)

    # Then headings and labels produce stable fields regardless of order.
    assert profile["Name"] == "Ada Lovelace"
    assert profile["Courses"] == (
        "Algorithms and Data Structures\nDiscrete Mathematics"
    )
    assert profile["Last Access"] == "Monday, 1 September 2026; 2 hours ago"
    assert profile["Timezone"] == "Europe/Skopje"


def test_old_instance_profile_fixture_parses_stable_fields() -> None:
    # Given representative HTML from the old Moodle instance.
    html = _fixture("profile_old.html")

    # When the profile is parsed with the old-instance selectors.
    profile = parse_profile_html(html, selectors_old)

    # Then the same field-label contract applies to its markup.
    assert profile["Name"] == "Grace Hopper"
    assert profile["Courses"] == "Compiler Construction"
    assert profile["Last Access"] == "Never"
    assert profile["Timezone"] == "UTC"


def test_displayed_text_is_trimmed_and_collapsed() -> None:
    # Given profile text split across indentation and nested elements.
    html = _fixture("profile_new.html")

    # When the profile is parsed.
    profile = parse_profile_html(html, selectors_new)

    # Then displayed text has stable single spaces and course separators.
    assert profile["Description"] == "Builds reliable analytical engines."
    assert profile["City"] == "Skopje Center"
    assert profile["Mail"] == "ada@example.edu"


def test_header_avatar_takes_precedence_over_detail_row() -> None:
    # Given a header avatar and a conflicting Avatar detail row.
    html = _fixture("profile_new.html")

    # When the profile is parsed.
    profile = parse_profile_html(html, selectors_new)

    # Then only the canonical header avatar is retained.
    assert profile["Avatar"] == "https://example.edu/header-avatar.png"


def test_default_header_avatar_is_omitted() -> None:
    # Given an old-instance profile using Moodle's default avatar class.
    html = _fixture("profile_old.html")

    # When the profile is parsed.
    profile = parse_profile_html(html, selectors_old)

    # Then the placeholder is represented as no avatar.
    assert profile["Avatar"] == ""


def test_profile_without_profile_tree_keeps_substantive_header_content() -> None:
    # Given a valid profile page without the optional profile tree.
    html = _fixture("profile_header_only.html")

    # When the profile is parsed.
    profile = parse_profile_html(html, selectors_new)

    # Then its substantive header and description are retained.
    assert profile["Name"] == "Katherine Johnson"
    assert profile["Description"] == "Orbital mechanics researcher."
