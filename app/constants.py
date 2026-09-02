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


class _BaseSelectors(TypedDict):
    name_selector: str
    description_selector: str
    description_images_selector: str
    courses_selector: str
    details_selector: str
    sections_selector: str
    attribute_selector: str
    avatar_selector: str


class Selectors(_BaseSelectors):
    interests_selector: str


_base_selectors: _BaseSelectors = {
    "name_selector": ".page-header-headings h1",
    "description_selector": "#region-main .description",
    "description_images_selector": "#region-main .description img",
    "courses_selector": "dd ul li",
    "details_selector": "li.contentnode",
    "sections_selector": "#region-main .profile_tree section",
    "attribute_selector": "h3.lead",
    "avatar_selector": ".page-header-image img",
}

selectors_new: Selectors = {
    **_base_selectors,
    "interests_selector": "dl > dd > div > ul > li > a",
}

selectors_old: Selectors = {
    **_base_selectors,
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
    "timezone": "Timezone",
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
    "Timezone",
    "Interests",
    "Web",
    "MoodleNet",
    "Skype",
    "MSN",
    "Yahoo",
    "ICQ",
    "AIM",
]
