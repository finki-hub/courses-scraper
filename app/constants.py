from typing import TypedDict

__all__ = [
    "Selectors",
    "base_urls",
    "columns",
    "fields",
    "selectors_new",
    "selectors_old",
]


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


selectors_new: Selectors = {
    "name_selector": (
        "#page-header > div > div > div > div.d-flex.align-items-center > "
        "div.me-auto > div > div.page-header-headings > h1"
    ),
    "description_selector": "#region-main > div > div > div.description",
    "description_images_selector": "#region-main > div > div > div.description img",
    "courses_selector": "ul > li > dl > dd > ul > li",
    "last_access_selector": (
        "#region-main > div > div > div.profile_tree > section:nth-child(4) > "
        "div > ul > li > dl > dd"
    ),
    "details_selector": "ul > li.contentnode",
    "sections_selector": "#region-main > div > div > div.profile_tree > section",
    "interests_selector": "dl > dd > div > ul > li > a",
    "attribute_selector": "h3.lead",
    "avatar_selector": ".page-header-image > img",
}

selectors_old: Selectors = {
    "name_selector": (
        "#page-header > div > div > div > div.d-flex.align-items-center > "
        "div.mr-auto > div > div.page-header-headings > h1"
    ),
    "description_selector": "#region-main > div > div > div.description",
    "description_images_selector": "#region-main > div > div > div.description img",
    "courses_selector": "ul > li > dl > dd > ul > li",
    "last_access_selector": "ul > li > dl > dd",
    "details_selector": "ul > li.contentnode",
    "sections_selector": "#region-main > div > div > div.profile_tree > section",
    "interests_selector": "li:not(.visibleifjs)",
    "attribute_selector": "h3.lead",
    "avatar_selector": ".page-header-image > img",
}

base_urls: dict[str, str] = {
    "new": "https://courses.finki.ukim.mk",
    "old": "https://oldcourses.finki.ukim.mk",
}

fields: dict[str, str] = {
    "email address": "Mail",
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
    "ID",
    "Name",
    "Mail",
    "Courses",
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
