import argparse
import logging
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from http import HTTPStatus
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from app.constants import (
    Selectors,
    base_urls,
    columns,
    fields,
    selectors_new,
    selectors_old,
)

logger = logging.getLogger(__name__)


def get_profile_name(element: Tag, selectors: Selectors) -> str:
    name = element.select_one(selectors["name_selector"])

    if name is None:
        return ""

    return name.text.strip()


def get_profile_avatar(element: Tag, selectors: Selectors) -> str:
    avatar = element.select_one(selectors["avatar_selector"])

    if avatar is None:
        return ""

    classes = avatar.get("class", [])
    if isinstance(classes, list) and "defaultuserpic" in classes:
        return ""

    src = avatar.get("src")
    if not isinstance(src, str):
        return ""

    return src


def get_profile_description(element: Tag, selectors: Selectors) -> str:
    description = element.select_one(selectors["description_selector"])

    if description is None:
        return ""

    return description.text.strip()


def get_profile_description_images(element: Tag, selectors: Selectors) -> str:
    images = element.select(selectors["description_images_selector"])

    return "\n".join(
        src for image in images if (src := image.get("src")) and isinstance(src, str)
    )


def get_profile_details(element: Tag, selectors: Selectors) -> dict[str, str]:
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


def get_profile_courses(element: Tag, selectors: Selectors) -> str:
    courses_tags = element.select(selectors["courses_selector"])
    courses = [li.text for li in courses_tags]

    return "\n".join(courses)


def get_profile_last_access(element: Tag, selectors: Selectors) -> str:
    last_access = element.select_one(selectors["last_access_selector"])

    if last_access is None:
        return ""

    return last_access.text.replace("\xa0", ";")


def get_profile_attributes(element: Tag, selectors: Selectors) -> dict[str, str]:
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
    selectors: Selectors,
) -> dict[str, str]:
    profile_url = f"{base_url}/user/profile.php?id={profile_id}&showallcourses=1"

    try:
        response = session.get(profile_url, timeout=(5, 15))
    except requests.exceptions.RequestException:
        logger.warning("Request failed for profile %d", profile_id, exc_info=True)
        return {}

    if response.status_code != HTTPStatus.OK:
        return {}

    soup = BeautifulSoup(response.text, "lxml")

    try:
        profile = get_profile_attributes(soup, selectors)
    except (AttributeError, KeyError, TypeError):
        logger.warning("Failed to parse profile %d", profile_id, exc_info=True)
        return {}

    if profile:
        profile["ID"] = str(profile_id)

    return profile


def get_profiles(
    session: requests.Session,
    profile_ids: range | list[int],
    threads: int,
    base_url: str,
    selectors: Selectors,
) -> list[dict[str, str]]:
    profiles: list[dict[str, str]] = []
    failed = 0

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {
            executor.submit(get_profile, session, pid, base_url, selectors): pid
            for pid in profile_ids
        }

        try:
            for future in tqdm(as_completed(futures), total=len(futures)):
                try:
                    result = future.result()
                except Exception:
                    pid = futures[future]
                    logger.warning(
                        "Unexpected error for profile %d",
                        pid,
                        exc_info=True,
                    )
                    failed += 1
                    continue
                if result:
                    profiles.append(result)
                else:
                    failed += 1
        except KeyboardInterrupt:
            logger.info(
                "Interrupted — cancelling pending futures and returning "
                "%d profiles collected so far from %s",
                len(profiles),
                base_url,
            )
            for pending in futures:
                pending.cancel()
            raise

    logger.info(
        "Scraped %d profiles from %s (%d empty/failed)",
        len(profiles),
        base_url,
        failed,
    )
    return profiles


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
    parser.add_argument("-t", type=int, default=10, help="How many threads to use")

    id_group = parser.add_mutually_exclusive_group(required=True)
    id_group.add_argument("-i", type=int, nargs="+", help="Profile IDs to scrape")
    id_group.add_argument("-m", type=int, help="Highest ID")

    return parser.parse_args()


def get_courses_session(cookie: str, threads: int = 10) -> requests.Session:
    retry_strategy = Retry(
        total=5,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=1,
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=max(10, threads),
        pool_maxsize=max(10, threads),
    )
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

    merged_df["Profile"] = (
        base_urls["new"] + "/user/profile.php?id=" + merged_df["ID"].astype(str)
    )

    if "Courses" in merged_df.columns:
        courses_series = merged_df["Courses"].fillna("")
        merged_df["Courses Count"] = (
            courses_series.where(
                courses_series != "",
                other="",
            )
            .str.split("\n")
            .apply(
                lambda parts: len([c for c in parts if c.strip()]),
            )
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


def _save_checkpoints(
    df_new: pd.DataFrame,
    df_old: pd.DataFrame,
    checkpoint_new: Path,
    checkpoint_old: Path,
) -> None:
    df_new.to_csv(checkpoint_new, index=False)
    df_old.to_csv(checkpoint_old, index=False)
    logger.info(
        "Checkpoints saved (%d new, %d old profiles)",
        len(df_new),
        len(df_old),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    args = parse_args()
    session_new = get_courses_session(args.c1, args.t)
    session_old = get_courses_session(args.c2, args.t)
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
        logger.info("Loading from checkpoints...")
        df_new = pd.read_csv(checkpoint_new)
        df_old = pd.read_csv(checkpoint_old)
        scraped_ids = set(df_new["ID"].astype(str)) | set(df_old["ID"].astype(str))
        remaining_ids = [pid for pid in profile_ids if str(pid) not in scraped_ids]

        if remaining_ids:
            logger.info(
                "Resuming scraping for %d remaining profiles...",
                len(remaining_ids),
            )

            resume_new: Future[list[dict[str, str]]] = Future()
            resume_old: Future[list[dict[str, str]]] = Future()
            try:
                with ThreadPoolExecutor(max_workers=2) as site_executor:
                    resume_new = site_executor.submit(
                        get_profiles,
                        session_new,
                        remaining_ids,
                        args.t,
                        base_urls["new"],
                        selectors_new,
                    )
                    resume_old = site_executor.submit(
                        get_profiles,
                        session_old,
                        remaining_ids,
                        args.t,
                        base_urls["old"],
                        selectors_old,
                    )
                    profiles_new_additional = resume_new.result()
                    profiles_old_additional = resume_old.result()
            except KeyboardInterrupt:
                logger.info("Interrupted — saving partial checkpoints...")
                partial_new: list[dict[str, str]] = []
                partial_old: list[dict[str, str]] = []
                try:
                    partial_new = resume_new.result(timeout=0)
                except Exception:
                    logger.debug("Could not retrieve new profiles", exc_info=True)
                try:
                    partial_old = resume_old.result(timeout=0)
                except Exception:
                    logger.debug("Could not retrieve old profiles", exc_info=True)
                df_new_additional = reorder_columns(
                    pd.DataFrame(partial_new),
                    columns,
                )
                df_old_additional = reorder_columns(
                    pd.DataFrame(partial_old),
                    columns,
                )
                df_new = pd.concat([df_new, df_new_additional], ignore_index=True)
                df_old = pd.concat([df_old, df_old_additional], ignore_index=True)
                _save_checkpoints(df_new, df_old, checkpoint_new, checkpoint_old)
                sys.exit(130)

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
            logger.info("All profiles already scraped.")
    else:
        logger.info("Scraping both instances concurrently...")

        future_new: Future[list[dict[str, str]]] = Future()
        future_old: Future[list[dict[str, str]]] = Future()
        try:
            with ThreadPoolExecutor(max_workers=2) as site_executor:
                future_new = site_executor.submit(
                    get_profiles,
                    session_new,
                    profile_ids,
                    args.t,
                    base_urls["new"],
                    selectors_new,
                )
                future_old = site_executor.submit(
                    get_profiles,
                    session_old,
                    profile_ids,
                    args.t,
                    base_urls["old"],
                    selectors_old,
                )
                profiles_new = future_new.result()
                profiles_old = future_old.result()
        except KeyboardInterrupt:
            logger.info("Interrupted — saving partial checkpoints...")
            salvaged_new: list[dict[str, str]] = []
            salvaged_old: list[dict[str, str]] = []
            try:
                salvaged_new = future_new.result(timeout=0)
            except Exception:
                logger.debug("Could not retrieve new profiles", exc_info=True)
            try:
                salvaged_old = future_old.result(timeout=0)
            except Exception:
                logger.debug("Could not retrieve old profiles", exc_info=True)
            df_new = reorder_columns(pd.DataFrame(salvaged_new), columns)
            df_old = reorder_columns(pd.DataFrame(salvaged_old), columns)
            _save_checkpoints(df_new, df_old, checkpoint_new, checkpoint_old)
            sys.exit(130)

        df_new = reorder_columns(pd.DataFrame(profiles_new), columns)
        df_new.to_csv(checkpoint_new, index=False)

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

    logger.info("\n%s", df_merged.tail())
    logger.info("Finished in %.2f seconds", time.time() - start)


if __name__ == "__main__":
    main()
