import argparse
import logging
import sys
import time
from concurrent.futures import (
    CancelledError,
    Future,
    ThreadPoolExecutor,
    as_completed,
)
from concurrent.futures import (
    TimeoutError as FuturesTimeoutError,
)
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from app.constants import (
    COL_COURSES,
    COL_ID,
    COL_MAIL,
    COL_NAME,
    COL_PROFILE,
    COURSES_COUNT,
    Selectors,
    base_urls,
    columns,
    fields,
    selectors_new,
    selectors_old,
)

logger = logging.getLogger(__name__)


@dataclass
class ScrapeConfig:
    session_new: requests.Session
    session_old: requests.Session
    threads: int
    checkpoint_new: Path
    checkpoint_old: Path


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

    profile[COL_NAME] = get_profile_name(element, selectors)
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
            profile[COL_COURSES] = get_profile_courses(section, selectors)
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
        profile[COL_ID] = str(profile_id)

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
            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=base_url.rsplit("//", maxsplit=1)[-1],
            ):
                try:
                    result = future.result()
                except CancelledError:
                    pid = futures[future]
                    logger.info("Profile %d fetch cancelled", pid)
                    failed += 1
                    continue
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


def reorder_columns(df: pd.DataFrame, col_order: list[str]) -> pd.DataFrame:
    for column in col_order:
        if column not in df.columns:
            df[column] = ""

    return df[col_order]


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


def _merge_field(
    merged_df: pd.DataFrame,
    field: str,
) -> pd.DataFrame:
    col_x = f"{field}_x"
    col_y = f"{field}_y"

    if col_x in merged_df.columns and col_y in merged_df.columns:
        merged_df[field] = merged_df[col_y].fillna(merged_df[col_x])
        merged_df = merged_df.drop(columns=[col_x, col_y])
    elif col_x in merged_df.columns:
        merged_df = merged_df.rename(columns={col_x: field})
    elif col_y in merged_df.columns:
        merged_df = merged_df.rename(columns={col_y: field})

    return merged_df


def _parse_courses(raw: str) -> list[str]:
    if pd.notna(raw) and raw:
        return [c.strip() for c in raw.split("\n") if c.strip()]
    return []


def _merge_courses(courses_old: str, courses_new: str) -> str:
    combined = _parse_courses(courses_new) + _parse_courses(courses_old)
    return "\n".join(dict.fromkeys(combined)) if combined else ""


def _merge_courses_column(merged_df: pd.DataFrame) -> pd.DataFrame:
    col_x = f"{COL_COURSES}_x"
    col_y = f"{COL_COURSES}_y"

    if col_x in merged_df.columns and col_y in merged_df.columns:
        merged_df[COL_COURSES] = [
            _merge_courses(x, y)
            for x, y in zip(merged_df[col_x], merged_df[col_y], strict=True)
        ]
        return merged_df.drop(columns=[col_x, col_y])

    return _merge_field(merged_df, COL_COURSES)


def _add_courses_count(merged_df: pd.DataFrame) -> pd.DataFrame:
    if COL_COURSES not in merged_df.columns:
        return merged_df

    courses_series = merged_df[COL_COURSES].fillna("")
    merged_df[COURSES_COUNT] = courses_series.apply(
        lambda c: len([p for p in c.split("\n") if p.strip()]) if c else 0,
    )

    return merged_df


def _build_column_order(
    merged_df: pd.DataFrame,
    single_fields: list[str],
) -> list[str]:
    base_columns = sorted(
        {
            col.replace("_old", "").replace("_new", "")
            for col in merged_df.columns
            if col not in [*single_fields, COL_PROFILE, COURSES_COUNT]
        },
    )

    new_order = [COL_ID]
    new_order.extend(
        col_name
        for col_name in (COL_PROFILE, COL_NAME, COL_MAIL, COL_COURSES, COURSES_COUNT)
        if col_name in merged_df.columns
    )

    for base_col in base_columns:
        if f"{base_col}_new" in merged_df.columns:
            new_order.append(f"{base_col}_new")
        if f"{base_col}_old" in merged_df.columns:
            new_order.append(f"{base_col}_old")

    return new_order


def merge_profiles(df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    single_fields = [COL_ID, COL_NAME, COL_MAIL, COL_COURSES]

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

    merged_df = df_old_renamed.merge(
        df_new_renamed,
        on=COL_ID,
        how="outer",
        validate="many_to_many",
    )

    for field in (COL_NAME, COL_MAIL):
        merged_df = _merge_field(merged_df, field)

    merged_df = _merge_courses_column(merged_df)
    merged_df = _add_courses_count(merged_df)

    merged_df[COL_PROFILE] = (
        base_urls["new"] + "/user/profile.php?id=" + merged_df[COL_ID].astype(str)
    )

    return merged_df[_build_column_order(merged_df, single_fields)]


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


def _salvage_futures(
    future_new: Future[list[dict[str, str]]],
    future_old: Future[list[dict[str, str]]],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    salvaged_new: list[dict[str, str]] = []
    salvaged_old: list[dict[str, str]] = []
    try:
        salvaged_new = future_new.result(timeout=0)
    except (CancelledError, FuturesTimeoutError):
        logger.debug("Could not retrieve new profiles (not ready or cancelled)")
    except Exception:
        logger.debug("Could not retrieve new profiles", exc_info=True)
    try:
        salvaged_old = future_old.result(timeout=0)
    except (CancelledError, FuturesTimeoutError):
        logger.debug("Could not retrieve old profiles (not ready or cancelled)")
    except Exception:
        logger.debug("Could not retrieve old profiles", exc_info=True)
    return salvaged_new, salvaged_old


def _scrape_with_interrupt_handling(
    config: ScrapeConfig,
    profile_ids: range | list[int],
    existing_new: pd.DataFrame | None = None,
    existing_old: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    future_new: Future[list[dict[str, str]]] = Future()
    future_old: Future[list[dict[str, str]]] = Future()

    try:
        with ThreadPoolExecutor(max_workers=2) as site_executor:
            future_new = site_executor.submit(
                get_profiles,
                config.session_new,
                profile_ids,
                config.threads,
                base_urls["new"],
                selectors_new,
            )
            future_old = site_executor.submit(
                get_profiles,
                config.session_old,
                profile_ids,
                config.threads,
                base_urls["old"],
                selectors_old,
            )
            profiles_new, profiles_old = (
                future_new.result(),
                future_old.result(),
            )
    except KeyboardInterrupt:
        logger.info("Interrupted — saving partial checkpoints...")
        partial_new, partial_old = _salvage_futures(future_new, future_old)
        df_new = reorder_columns(pd.DataFrame(partial_new), columns)
        df_old = reorder_columns(pd.DataFrame(partial_old), columns)

        if existing_new is not None:
            df_new = pd.concat([existing_new, df_new], ignore_index=True)
        if existing_old is not None:
            df_old = pd.concat([existing_old, df_old], ignore_index=True)

        _save_checkpoints(
            df_new,
            df_old,
            config.checkpoint_new,
            config.checkpoint_old,
        )
        sys.exit(130)

    df_new = reorder_columns(pd.DataFrame(profiles_new), columns)
    df_old = reorder_columns(pd.DataFrame(profiles_old), columns)

    if existing_new is not None:
        df_new = pd.concat([existing_new, df_new], ignore_index=True)
    if existing_old is not None:
        df_old = pd.concat([existing_old, df_old], ignore_index=True)

    return df_new, df_old


def _resolve_profile_ids(
    args: argparse.Namespace,
) -> range | list[int] | None:
    if args.i is not None:
        return list[int](args.i)
    if args.m is not None:
        return range(1, args.m + 1)
    return None


def _resume_from_checkpoints(
    config: ScrapeConfig,
    profile_ids: range | list[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Loading from checkpoints...")
    df_new = pd.read_csv(config.checkpoint_new)
    df_old = pd.read_csv(config.checkpoint_old)
    scraped_ids = set(df_new[COL_ID].astype(str)) | set(df_old[COL_ID].astype(str))
    remaining_ids = [pid for pid in profile_ids if str(pid) not in scraped_ids]

    if not remaining_ids:
        logger.info("All profiles already scraped.")
        return df_new, df_old

    logger.info(
        "Resuming scraping for %d remaining profiles...",
        len(remaining_ids),
    )

    return _scrape_with_interrupt_handling(
        config,
        remaining_ids,
        existing_new=df_new,
        existing_old=df_old,
    )


def _finalize_output(
    df_old: pd.DataFrame,
    df_new: pd.DataFrame,
    output_path: Path,
    output_file: str,
    config: ScrapeConfig,
) -> None:
    df_merged = merge_profiles(df_old, df_new)
    df_merged[COL_ID] = df_merged[COL_ID].astype(int)
    df_merged = df_merged.sort_values(COL_ID)
    df_merged[COL_ID] = df_merged[COL_ID].astype(str)
    df_merged.to_csv(output_path / output_file, index=False)

    if config.checkpoint_new.exists():
        config.checkpoint_new.unlink()
    if config.checkpoint_old.exists():
        config.checkpoint_old.unlink()

    logger.info("Written %d profiles to %s", len(df_merged), output_path / output_file)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logging.getLogger("urllib3").setLevel(logging.ERROR)

    args = parse_args()
    start = time.time()

    profile_ids = _resolve_profile_ids(args)
    if profile_ids is None:
        return

    output_path = Path("output")
    output_path.mkdir(exist_ok=True, parents=True)

    config = ScrapeConfig(
        session_new=get_courses_session(args.c1, args.t),
        session_old=get_courses_session(args.c2, args.t),
        threads=args.t,
        checkpoint_new=output_path / "checkpoint_new.csv",
        checkpoint_old=output_path / "checkpoint_old.csv",
    )

    if config.checkpoint_new.exists() and config.checkpoint_old.exists():
        df_new, df_old = _resume_from_checkpoints(config, profile_ids)
    else:
        logger.info("Scraping both instances concurrently...")
        df_new, df_old = _scrape_with_interrupt_handling(
            config,
            profile_ids,
        )
        df_new.to_csv(config.checkpoint_new, index=False)
        df_old.to_csv(config.checkpoint_old, index=False)

    _finalize_output(df_old, df_new, output_path, args.o, config)
    logger.info("Finished in %.2f seconds", time.time() - start)


if __name__ == "__main__":
    main()
