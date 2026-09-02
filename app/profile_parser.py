from bs4 import BeautifulSoup, Tag

from app.constants import COL_COURSES, COL_NAME, Selectors, fields

__all__ = ["parse_profile_html"]


def parse_profile_html(html: str, selectors: Selectors) -> dict[str, str]:
    soup = BeautifulSoup(html, "lxml")
    profile = {
        COL_NAME: _selected_text(soup, selectors["name_selector"]),
        "Description": _selected_text(soup, selectors["description_selector"]),
        "Images": _description_images(soup, selectors),
        "Avatar": _avatar(soup, selectors),
    }

    for section in soup.select(selectors["sections_selector"]):
        heading = section.select_one(selectors["attribute_selector"])
        if heading is None:
            continue

        heading_text = _display_text(heading).casefold()
        if heading_text == "user details":
            profile.update(_details(section, selectors))
            continue
        if heading_text == "course details":
            profile[COL_COURSES] = _courses(section, selectors)
            continue
        if heading_text == "login activity":
            profile["Last Access"] = _last_access(section, selectors)

    substantive = (
        value for field, value in profile.items() if field not in {"Images", "Avatar"}
    )
    return profile if any(substantive) else {}


def _display_text(element: Tag) -> str:
    return " ".join(element.get_text(" ", strip=True).split())


def _selected_text(element: Tag, selector: str) -> str:
    selected = element.select_one(selector)
    return "" if selected is None else _display_text(selected)


def _description_images(element: Tag, selectors: Selectors) -> str:
    sources: list[str] = []
    for image in element.select(selectors["description_images_selector"]):
        source = image.get("src")
        if isinstance(source, str):
            sources.append(source)
    return "\n".join(sources)


def _avatar(element: Tag, selectors: Selectors) -> str:
    avatar = element.select_one(selectors["avatar_selector"])
    if avatar is None:
        return ""

    classes = avatar.get("class")
    if isinstance(classes, list) and "defaultuserpic" in classes:
        return ""

    source = avatar.get("src")
    return source if isinstance(source, str) else ""


def _details(element: Tag, selectors: Selectors) -> dict[str, str]:
    attributes: dict[str, str] = {}
    for detail in element.select(selectors["details_selector"]):
        field_element = detail.select_one("dt")
        value_element = detail.select_one("dd")
        if field_element is None:
            continue
        if value_element is None:
            continue

        field = _display_text(field_element).casefold()
        output_field = fields.get(field)
        if output_field is None:
            continue

        value = _display_text(value_element)
        if field == "interests":
            value = "\n".join(
                _display_text(interest)
                for interest in value_element.select(selectors["interests_selector"])
            )
        if field == "email address":
            value = value.removesuffix(" (Visible to other course participants)")
        attributes[output_field] = value
    return attributes


def _courses(element: Tag, selectors: Selectors) -> str:
    return "\n".join(
        course
        for course_element in element.select(selectors["courses_selector"])
        if (course := _display_text(course_element))
    )


def _last_access(element: Tag, selectors: Selectors) -> str:
    for detail in element.select(selectors["details_selector"]):
        field_element = detail.select_one("dt")
        value_element = detail.select_one("dd")
        if field_element is None:
            continue
        if value_element is None:
            continue
        if _display_text(field_element).casefold() == "last access":
            value = value_element.get_text(" ", strip=True).replace("\xa0", "; ")
            return " ".join(value.split())
    return ""
