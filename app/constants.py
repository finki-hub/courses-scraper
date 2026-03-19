from typing import TypedDict

__all__ = [
    "COL_COURSES",
    "COL_ID",
    "COL_MAIL",
    "COL_NAME",
    "COL_PROFILE",
    "COURSES_COUNT",
    "Selectors",
    "base_urls",
    "columns",
    "fields",
    "selectors_new",
    "selectors_old",
]

COL_ID = "ID"
COL_NAME = "Name"
COL_MAIL = "Mail"
COL_COURSES = "Courses"
COL_PROFILE = "Profile"

COURSES_COUNT = "Courses Count"


class Selectors(TypedDict):
    name_selector: str
    description_selector: str
    description_images_selector: str
    courses_selector: str
    last_access_selector: str
    details_selector: str
    sections_selector: str
    interests_selector: str
    attribute_selector: str
    avatar_selector: str


_base_selectors = {
    "description_selector": "#region-main > div > div > div.description",
    "description_images_selector": "#region-main > div > div > div.description img",
    "courses_selector": "ul > li > dl > dd > ul > li",
    "details_selector": "ul > li.contentnode",
    "sections_selector": "#region-main > div > div > div.profile_tree > section",
    "attribute_selector": "h3.lead",
    "avatar_selector": ".page-header-image > img",
}

selectors_new: Selectors = {
    **_base_selectors,  # type: ignore[typeddict-item]
    "name_selector": (
        "#page-header > div > div > div > div.d-flex.align-items-center > "
        "div.me-auto > div > div.page-header-headings > h1"
    ),
    "last_access_selector": (
        "#region-main > div > div > div.profile_tree > section:nth-child(4) > "
        "div > ul > li > dl > dd"
    ),
    "interests_selector": "dl > dd > div > ul > li > a",
}

selectors_old: Selectors = {
    **_base_selectors,  # type: ignore[typeddict-item]
    "name_selector": (
        "#page-header > div > div > div > div.d-flex.align-items-center > "
        "div.mr-auto > div > div.page-header-headings > h1"
    ),
    "last_access_selector": "ul > li > dl > dd",
    "interests_selector": "li:not(.visibleifjs)",
}

base_urls: dict[str, str] = {
    "new": "https://courses.finki.ukim.mk",
    "old": "https://oldcourses.finki.ukim.mk",
}

fields: dict[str, str] = {
    "email address": COL_MAIL,
    "web page": "Web",
    "interests": "Interests",
    "icq number": "ICQ",
    "skype id": "Skype",
    "yahoo id": "Yahoo",
    "aim id": "AIM",
    "msn id": "MSN",
    "country": "Country",
    "city/town": "City",
    "moodlenet profile": "MoodleNet",
    "avatar": "Avatar",
}

columns: list[str] = [
    COL_ID,
    COL_NAME,
    COL_MAIL,
    COL_COURSES,
    "Last Access",
    "Avatar",
    "Description",
    "Images",
    "Country",
    "City",
    "Interests",
    "Web",
    "MoodleNet",
    "Skype",
    "MSN",
    "Yahoo",
    "ICQ",
    "AIM",
]
