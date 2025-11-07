import argparse
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from app.constants import (
    base_urls,
    columns,
    fields,
    http_ok,
    selectors_new,
    selectors_old,
)


def get_profile_name(element: Tag, selectors: dict) -> str:
    name = element.select_one(selectors["name_selector"])

    if name is None:
        return ""

    return name.text


def get_profile_avatar(element: Tag, selectors: dict) -> str:
    avatar = element.select_one(selectors["avatar_selector"])

    if avatar is None or "defaultuserpic" in avatar.attrs["class"]:
        return ""

    return avatar.attrs["src"]


def get_profile_description(element: Tag, selectors: dict) -> str:
    description = element.select_one(selectors["description_selector"])

    if description is None:
        return ""

    return description.text


def get_profile_description_images(element: Tag, selectors: dict) -> str:
    images = element.select(selectors["description_images_selector"])

    return "\n".join([image.attrs["src"] for image in images])


def get_profile_details(element: Tag, selectors: dict) -> dict[str, str]:
    attributes: dict[str, str] = {}
    details = element.select(selectors["details_selector"])

    for detail in details:
        field_element = detail.dt
        value_element = detail.dd

        if field_element is None or value_element is None:
            continue

        field = field_element.text.strip().lower()

        if field in fields:
            value = value_element.text.strip()

            if field == "interests":
                interests = value_element.select(selectors["interests_selector"])
                value = "\n".join(interest.text.strip() for interest in interests)
            elif field == "email address":
                value = value.replace(" (Visible to other course participants)", "")

            attributes[fields[field]] = value

    return attributes


def get_profile_courses(element: Tag, selectors: dict) -> str:
    courses_tags = element.select(selectors["courses_selector"])
    courses = [li.text for li in courses_tags]

    return "\n".join(courses)


def get_profile_last_access(element: Tag, selectors: dict) -> str:
    last_access = element.select_one(selectors["last_access_selector"])

    if last_access is None:
        return ""

    return last_access.text.replace("\xa0", ";")


def get_profile_attributes(element: Tag, selectors: dict) -> dict[str, str]:
    profile: dict[str, str] = {}
    sections = element.select(selectors["sections_selector"])

    if len(sections) == 0:
        return {}

    profile["Name"] = get_profile_name(element, selectors)
    profile["Description"] = get_profile_description(element, selectors)
    profile["Images"] = get_profile_description_images(element, selectors)
    profile["Avatar"] = get_profile_avatar(element, selectors)

    for section in sections:
        attribute = section.select_one(selectors["attribute_selector"])

        if attribute is None:
            continue

        if attribute.text == "User details":
            profile |= get_profile_details(section, selectors)
        elif attribute.text == "Course details":
            profile["Courses"] = get_profile_courses(section, selectors)
        elif attribute.text == "Login activity":
            profile["Last Access"] = get_profile_last_access(section, selectors)

    return profile


def get_profile(
    session: requests.Session,
    profile_id: int,
    base_url: str,
    selectors: dict,
) -> dict[str, str]:
    profile_url = f"{base_url}/user/profile.php?id={profile_id}&showallcourses=1"
    response = session.get(profile_url)

    if response.status_code != http_ok:
        return {}

    soup = BeautifulSoup(response.text, "html.parser")

    try:
        profile = get_profile_attributes(soup, selectors)
    except Exception:
        return {}

    if profile:
        profile["ID"] = str(profile_id)

    return profile


def get_lambda(
    session: requests.Session,
    base_url: str,
    selectors: dict,
) -> Callable[[int], dict[str, str]]:
    return lambda x: get_profile(session, x, base_url, selectors)


def get_profiles(
    session: requests.Session,
    profile_ids: range | list[int],
    threads: int,
    base_url: str,
    selectors: dict,
) -> list[dict[str, str]]:
    with ThreadPoolExecutor(max_workers=threads) as executor:
        profiles = list(
            tqdm(
                executor.map(get_lambda(session, base_url, selectors), profile_ids),
                total=len(profile_ids),
            ),
        )

    return list(filter(len, profiles))


def reorder_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df[column] = ""

    return df[columns]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Courses profiles from two instances",
    )

    parser.add_argument(
        "-c1",
        type=str,
        required=True,
        help="New Courses instance session cookie",
    )
    parser.add_argument(
        "-c2",
        type=str,
        required=True,
        help="Old Courses instance session cookie",
    )
    parser.add_argument("-o", type=str, default="profiles.csv", help="Output file")
    parser.add_argument("-t", type=int, default="10", help="How many threads to use")

    id_group = parser.add_mutually_exclusive_group(required=True)
    id_group.add_argument("-i", type=int, nargs="+", help="Profile IDs to scrape")
    id_group.add_argument("-m", type=int, help="Highest ID")

    return parser.parse_args()


def get_courses_session(cookie: str) -> requests.Session:
    retry_strategy = Retry(
        total=5,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=4,
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.cookies.set("MoodleSession", cookie)

    return session


def merge_profiles(df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    single_fields = ["ID", "Name", "Mail", "Courses"]

    df_old_renamed = df_old.rename(
        columns={
            col: f"{col}_old" for col in df_old.columns if col not in single_fields
        },
    )
    df_new_renamed = df_new.rename(
        columns={
            col: f"{col}_new" for col in df_new.columns if col not in single_fields
        },
    )

    merged_df = pd.merge(df_old_renamed, df_new_renamed, on="ID", how="outer")

    for field in ["Name", "Mail"]:
        if f"{field}_x" in merged_df.columns and f"{field}_y" in merged_df.columns:
            merged_df[field] = merged_df[f"{field}_y"].fillna(merged_df[f"{field}_x"])
            merged_df = merged_df.drop(columns=[f"{field}_x", f"{field}_y"])
        elif f"{field}_x" in merged_df.columns:
            merged_df = merged_df.rename(columns={f"{field}_x": field})
        elif f"{field}_y" in merged_df.columns:
            merged_df = merged_df.rename(columns={f"{field}_y": field})

    def merge_courses(courses_old: str, courses_new: str) -> str:
        courses_new_list = []
        courses_old_list = []

        if pd.notna(courses_new) and courses_new:
            courses_new_list = [c.strip() for c in courses_new.split("\n") if c.strip()]
        if pd.notna(courses_old) and courses_old:
            courses_old_list = [c.strip() for c in courses_old.split("\n") if c.strip()]

        seen = set()
        result = []

        for course in courses_new_list:
            if course not in seen:
                seen.add(course)
                result.append(course)

        for course in courses_old_list:
            if course not in seen:
                seen.add(course)
                result.append(course)

        return "\n".join(result) if result else ""

    if "Courses_x" in merged_df.columns and "Courses_y" in merged_df.columns:
        merged_df["Courses"] = merged_df.apply(
            lambda row: merge_courses(row["Courses_x"], row["Courses_y"]),
            axis=1,
        )
        merged_df = merged_df.drop(columns=["Courses_x", "Courses_y"])
    elif "Courses_x" in merged_df.columns:
        merged_df = merged_df.rename(columns={"Courses_x": "Courses"})
    elif "Courses_y" in merged_df.columns:
        merged_df = merged_df.rename(columns={"Courses_y": "Courses"})

    merged_df["Profile"] = merged_df["ID"].apply(
        lambda profile_id: f"{base_urls['new']}/user/profile.php?id={profile_id}",
    )

    if "Courses" in merged_df.columns:
        merged_df["Courses Count"] = merged_df["Courses"].apply(
            lambda courses: (
                len([c for c in courses.split("\n") if c.strip()])
                if pd.notna(courses) and courses
                else 0
            ),
        )

    base_columns = sorted(
        {
            col.replace("_old", "").replace("_new", "")
            for col in merged_df.columns
            if col not in [*single_fields, "Profile", "Courses Count"]
        },
    )

    new_order = ["ID"]
    if "Profile" in merged_df.columns:
        new_order.append("Profile")
    if "Name" in merged_df.columns:
        new_order.append("Name")
    if "Mail" in merged_df.columns:
        new_order.append("Mail")
    if "Courses" in merged_df.columns:
        new_order.append("Courses")
    if "Courses Count" in merged_df.columns:
        new_order.append("Courses Count")

    for base_col in base_columns:
        if f"{base_col}_new" in merged_df.columns:
            new_order.append(f"{base_col}_new")
        if f"{base_col}_old" in merged_df.columns:
            new_order.append(f"{base_col}_old")

    merged_df = merged_df[new_order]

    return merged_df


def main() -> None:
    args = parse_args()
    session_new = get_courses_session(args.c1)
    session_old = get_courses_session(args.c2)
    start = time.time()

    profile_ids = None
    if args.i is not None:
        profile_ids = args.i
    elif args.m is not None:
        profile_ids = range(1, args.m + 1)
    else:
        return

    output_path = Path("output")
    output_path.mkdir(exist_ok=True, parents=True)

    checkpoint_new = output_path / "checkpoint_new.csv"
    checkpoint_old = output_path / "checkpoint_old.csv"

    if checkpoint_new.exists() and checkpoint_old.exists():
        print("Loading from checkpoints...")
        df_new = pd.read_csv(checkpoint_new)
        df_old = pd.read_csv(checkpoint_old)
        scraped_ids = set(df_new["ID"].astype(str))
        remaining_ids = [pid for pid in profile_ids if str(pid) not in scraped_ids]

        if remaining_ids:
            print(f"Resuming scraping for {len(remaining_ids)} remaining profiles...")
            profiles_new_additional = get_profiles(
                session_new,
                remaining_ids,
                args.t,
                base_urls["new"],
                selectors_new,
            )
            profiles_old_additional = get_profiles(
                session_old,
                remaining_ids,
                args.t,
                base_urls["old"],
                selectors_old,
            )

            df_new_additional = reorder_columns(
                pd.DataFrame(profiles_new_additional),
                columns,
            )
            df_old_additional = reorder_columns(
                pd.DataFrame(profiles_old_additional),
                columns,
            )

            df_new = pd.concat([df_new, df_new_additional], ignore_index=True)
            df_old = pd.concat([df_old, df_old_additional], ignore_index=True)
        else:
            print("All profiles already scraped.")
    else:
        print("Scraping new instance...")
        profiles_new = get_profiles(
            session_new,
            profile_ids,
            args.t,
            base_urls["new"],
            selectors_new,
        )
        df_new = reorder_columns(pd.DataFrame(profiles_new), columns)
        df_new.to_csv(checkpoint_new, index=False)

        print("Scraping old instance...")
        profiles_old = get_profiles(
            session_old,
            profile_ids,
            args.t,
            base_urls["old"],
            selectors_old,
        )
        df_old = reorder_columns(pd.DataFrame(profiles_old), columns)
        df_old.to_csv(checkpoint_old, index=False)

    df_merged = merge_profiles(df_old, df_new)
    df_merged["ID"] = df_merged["ID"].astype(int)
    df_merged = df_merged.sort_values("ID")
    df_merged["ID"] = df_merged["ID"].astype(str)
    df_merged.to_csv(output_path / args.o, index=False)

    if checkpoint_new.exists():
        checkpoint_new.unlink()
    if checkpoint_old.exists():
        checkpoint_old.unlink()

    print(df_merged.tail())
    print(f"Finished in {time.time() - start} seconds")


if __name__ == "__main__":
    main()
