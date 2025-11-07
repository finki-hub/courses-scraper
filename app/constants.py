selectors_new = {
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

selectors_old = {
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

base_urls = {
    "new": "https://courses.finki.ukim.mk",
    "old": "https://oldcourses.finki.ukim.mk",
}

fields = {
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

columns = [
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

http_ok = 200
