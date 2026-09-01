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
from pathlib import Path
from threading import Lock, local
from typing import assert_never

import pandas as pd
import requests
from tqdm import tqdm

from app.constants import (
    COL_ID,
    base_urls,
    columns,
    selectors_new,
    selectors_old,
)
from app.csv_io import write_csv_atomically
from app.http import (
    TRANSPORT_FAILURE_THRESHOLD,
    InstanceHttpConfig,
    ProfileEmpty,
    ProfileSuccess,
    ProfileTransportFailure,
    TransportFailureLimitError,
    create_session,
    fetch_profile,
    preflight_instance,
)
from app.profile_merge import merge_profiles

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ScrapeConfig:
    http_new: InstanceHttpConfig
    http_old: InstanceHttpConfig
    checkpoint_new: Path
    checkpoint_old: Path


class _WorkerSession(local):
    session: requests.Session | None

    def __init__(self) -> None:
        self.session = None


def get_profiles(
    config: InstanceHttpConfig,
    profile_ids: range | list[int],
) -> list[dict[str, str]]:
    profiles: list[dict[str, str]] = []
    profile_ids = list(profile_ids)
    empty_count = 0
    transport_failures = 0
    worker_state = _WorkerSession()
    worker_sessions: list[requests.Session] = []
    worker_sessions_lock = Lock()

    def fetch_with_worker_session(
        profile_id: int,
    ) -> ProfileSuccess | ProfileEmpty | ProfileTransportFailure:
        if worker_state.session is None:
            worker_state.session = create_session(config)
            with worker_sessions_lock:
                worker_sessions.append(worker_state.session)
        return fetch_profile(worker_state.session, profile_id, config)

    try:
        with ThreadPoolExecutor(max_workers=config.threads) as executor:
            futures = {
                executor.submit(fetch_with_worker_session, profile_id): profile_id
                for profile_id in profile_ids
            }

            try:
                for future in tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc=config.base_url.rsplit("//", maxsplit=1)[-1],
                ):
                    try:
                        outcome = future.result()
                    except CancelledError:
                        logger.info("Profile %d fetch cancelled", futures[future])
                        empty_count += 1
                        continue

                    match outcome:
                        case ProfileSuccess(profile=profile):
                            profiles.append(profile)
                        case ProfileEmpty():
                            empty_count += 1
                        case ProfileTransportFailure(profile_id=profile_id):
                            transport_failures += 1
                            logger.warning(
                                "Transport failed for profile %d from %s",
                                profile_id,
                                config.base_url,
                            )
                            if transport_failures >= TRANSPORT_FAILURE_THRESHOLD:
                                for pending in futures:
                                    pending.cancel()
                                raise TransportFailureLimitError(
                                    base_url=config.base_url,
                                    failure_count=transport_failures,
                                    threshold=TRANSPORT_FAILURE_THRESHOLD,
                                )
                        case unreachable:
                            assert_never(unreachable)
            except KeyboardInterrupt:
                logger.info(
                    "Interrupted — cancelling pending futures and returning "
                    "%d profiles collected so far from %s",
                    len(profiles),
                    config.base_url,
                )
                for pending in futures:
                    pending.cancel()
                raise
    finally:
        for session in worker_sessions:
            session.close()

    if transport_failures == len(profile_ids) and transport_failures > 0:
        raise TransportFailureLimitError(
            base_url=config.base_url,
            failure_count=transport_failures,
            threshold=TRANSPORT_FAILURE_THRESHOLD,
        )

    logger.info(
        "Scraped %d profiles from %s (%d empty, %d transport failures)",
        len(profiles),
        config.base_url,
        empty_count,
        transport_failures,
    )
    return profiles


def reorder_columns(df: pd.DataFrame, col_order: list[str]) -> pd.DataFrame:
    for column in col_order:
        if column not in df.columns:
            df[column] = ""

    return df.loc[:, col_order].copy()


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


def _save_checkpoints(
    df_new: pd.DataFrame,
    df_old: pd.DataFrame,
    checkpoint_new: Path,
    checkpoint_old: Path,
) -> None:
    write_csv_atomically(df_new, checkpoint_new)
    write_csv_atomically(df_old, checkpoint_old)
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
    profile_ids_new: range | list[int],
    profile_ids_old: range | list[int],
    existing_new: pd.DataFrame | None = None,
    existing_old: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    future_new: Future[list[dict[str, str]]] = Future()
    future_old: Future[list[dict[str, str]]] = Future()

    try:
        for http_config in (config.http_new, config.http_old):
            with create_session(http_config) as session:
                preflight_instance(session, http_config)

        with ThreadPoolExecutor(max_workers=2) as site_executor:
            future_new = site_executor.submit(
                get_profiles,
                config.http_new,
                profile_ids_new,
            )
            future_old = site_executor.submit(
                get_profiles,
                config.http_old,
                profile_ids_old,
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
        return list(dict.fromkeys(args.i))
    if args.m is not None:
        return range(1, args.m + 1)
    return None


def _remaining_profile_ids(
    profile_ids: range | list[int],
    checkpoint: pd.DataFrame,
) -> list[int]:
    scraped_ids = set(checkpoint[COL_ID].astype(str))
    return [
        profile_id for profile_id in profile_ids if str(profile_id) not in scraped_ids
    ]


def _load_checkpoint(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns, dtype="string")
    return reorder_columns(
        pd.read_csv(path, dtype="string", keep_default_na=False),
        columns,
    )


def _resume_from_checkpoints(
    config: ScrapeConfig,
    profile_ids: range | list[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Loading from checkpoints...")
    df_new = _load_checkpoint(config.checkpoint_new)
    df_old = _load_checkpoint(config.checkpoint_old)
    remaining_new = _remaining_profile_ids(profile_ids, df_new)
    remaining_old = _remaining_profile_ids(profile_ids, df_old)

    if not remaining_new and not remaining_old:
        logger.info("All profiles already scraped.")
        return df_new, df_old

    logger.info(
        "Resuming scraping for %d new and %d old remaining profiles...",
        len(remaining_new),
        len(remaining_old),
    )

    return _scrape_with_interrupt_handling(
        config,
        remaining_new,
        remaining_old,
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
    write_csv_atomically(df_merged, output_path / output_file)

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
        http_new=InstanceHttpConfig(
            base_url=base_urls["new"],
            cookie=args.c1,
            selectors=selectors_new,
            threads=args.t,
        ),
        http_old=InstanceHttpConfig(
            base_url=base_urls["old"],
            cookie=args.c2,
            selectors=selectors_old,
            threads=args.t,
        ),
        checkpoint_new=output_path / "checkpoint_new.csv",
        checkpoint_old=output_path / "checkpoint_old.csv",
    )

    if config.checkpoint_new.exists() or config.checkpoint_old.exists():
        df_new, df_old = _resume_from_checkpoints(config, profile_ids)
    else:
        logger.info("Scraping both instances concurrently...")
        df_new, df_old = _scrape_with_interrupt_handling(
            config,
            profile_ids,
            profile_ids,
        )
        _save_checkpoints(
            df_new,
            df_old,
            config.checkpoint_new,
            config.checkpoint_old,
        )

    _finalize_output(df_old, df_new, output_path, args.o, config)
    logger.info("Finished in %.2f seconds", time.time() - start)


if __name__ == "__main__":
    main()
