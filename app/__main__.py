import logging
import os
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, NoReturn

import pandas as pd

from app.checkpoints import (
    CheckpointPaths,
    CheckpointSnapshot,
    InstanceCheckpoint,
)
from app.checkpoints import clear as clear_checkpoints
from app.checkpoints import load as load_checkpoint
from app.checkpoints import save as save_checkpoint
from app.cli import parse_cli
from app.constants import (
    COL_ID,
    base_urls,
    selectors_new,
    selectors_old,
)
from app.coordinator import CoordinatorPlan
from app.coordinator import run as run_coordinator
from app.export import export_profiles
from app.http import (
    InstanceHttpConfig,
    create_session,
    fetch_profile,
)
from app.profile_collection import (
    ProfileCollectionDependencies,
    collect_profiles,
)
from app.profile_merge import merge_profiles

logger = logging.getLogger(__name__)
CHECKPOINT_BATCH_SIZE: Final = 100
CHECKPOINT_INTERVAL_SECONDS: Final = 30.0
POLL_INTERVAL_SECONDS: Final = 0.25


@dataclass(frozen=True, slots=True)
class ScrapeConfig:
    http_new: InstanceHttpConfig
    http_old: InstanceHttpConfig
    checkpoint_new: Path
    checkpoint_old: Path


def get_profiles(
    config: InstanceHttpConfig,
    profile_ids: Sequence[int],
) -> list[dict[str, str]]:
    return collect_profiles(
        config,
        profile_ids,
        ProfileCollectionDependencies(
            create_session=create_session,
            fetch_profile=fetch_profile,
        ),
    )


def _checkpoint_paths(config: ScrapeConfig) -> CheckpointPaths:
    return CheckpointPaths(
        manifest=config.checkpoint_new.parent / "checkpoint_manifest.json",
        legacy_new=config.checkpoint_new,
        legacy_old=config.checkpoint_old,
    )


def _terminate_process(exit_code: int) -> NoReturn:
    logging.shutdown()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(exit_code)


@dataclass(frozen=True, slots=True)
class _ScrapeState:
    requested_ids: tuple[int, ...]
    new: InstanceCheckpoint
    old: InstanceCheckpoint


def _scrape_with_interrupt_handling(
    config: ScrapeConfig,
    profile_ids_new: Sequence[int],
    profile_ids_old: Sequence[int],
    state: _ScrapeState | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    initial = None
    if state is not None:
        initial = CheckpointSnapshot(state.requested_ids, state.new, state.old)
    snapshot = run_coordinator(
        CoordinatorPlan(
            http_new=config.http_new,
            http_old=config.http_old,
            profile_ids_new=profile_ids_new,
            profile_ids_old=profile_ids_old,
            paths=_checkpoint_paths(config),
            initial=initial,
            batch_size=CHECKPOINT_BATCH_SIZE,
            poll_interval=POLL_INTERVAL_SECONDS,
            checkpoint_interval=CHECKPOINT_INTERVAL_SECONDS,
            monotonic_clock=time.monotonic,
            terminate=_terminate_process,
        ),
    )
    return snapshot.new.frame, snapshot.old.frame


def _remaining_profile_ids(
    profile_ids: Sequence[int],
    checkpoint: pd.DataFrame,
) -> list[int]:
    scraped_ids = set(checkpoint[COL_ID].astype(str))
    return [
        profile_id for profile_id in profile_ids if str(profile_id) not in scraped_ids
    ]


def _resume_from_checkpoints(
    config: ScrapeConfig,
    profile_ids: Sequence[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Loading from checkpoints...")
    paths = _checkpoint_paths(config)
    snapshot = load_checkpoint(paths, tuple(profile_ids))
    if snapshot is None:
        return _scrape_with_interrupt_handling(
            config,
            profile_ids,
            profile_ids,
        )
    remaining_new = [
        profile_id
        for profile_id in profile_ids
        if profile_id not in snapshot.new.completed_ids
    ]
    remaining_old = [
        profile_id
        for profile_id in profile_ids
        if profile_id not in snapshot.old.completed_ids
    ]

    if not remaining_new and not remaining_old:
        logger.info("All profiles already scraped.")
        save_checkpoint(paths, snapshot)
        return snapshot.new.frame, snapshot.old.frame

    logger.info(
        "Resuming scraping for %d new and %d old remaining profiles...",
        len(remaining_new),
        len(remaining_old),
    )

    return _scrape_with_interrupt_handling(
        config,
        remaining_new,
        remaining_old,
        _ScrapeState(
            requested_ids=snapshot.requested_ids,
            new=snapshot.new,
            old=snapshot.old,
        ),
    )


def _finalize_output(
    df_old: pd.DataFrame,
    df_new: pd.DataFrame,
    output_path: Path,
    output_file: str,
    config: ScrapeConfig,
) -> None:
    df_merged = merge_profiles(df_old, df_new)
    export_profiles(df_merged, output_path / output_file)

    clear_checkpoints(_checkpoint_paths(config))

    logger.info("Written %d profiles to %s", len(df_merged), output_path / output_file)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logging.getLogger("urllib3").setLevel(logging.ERROR)

    cli_config = parse_cli()
    start = time.time()
    profile_ids = cli_config.profile_ids

    output_path = cli_config.output_file.parent
    output_path.mkdir(exist_ok=True, parents=True)

    config = ScrapeConfig(
        http_new=InstanceHttpConfig(
            base_url=base_urls["new"],
            cookie=cli_config.cookie_new,
            selectors=selectors_new,
            threads=cli_config.threads,
        ),
        http_old=InstanceHttpConfig(
            base_url=base_urls["old"],
            cookie=cli_config.cookie_old,
            selectors=selectors_old,
            threads=cli_config.threads,
        ),
        checkpoint_new=output_path / "checkpoint_new.csv",
        checkpoint_old=output_path / "checkpoint_old.csv",
    )

    checkpoint_paths = _checkpoint_paths(config)
    if (
        checkpoint_paths.manifest.exists()
        or checkpoint_paths.legacy_new.exists()
        or checkpoint_paths.legacy_old.exists()
    ):
        df_new, df_old = _resume_from_checkpoints(config, profile_ids)
    else:
        logger.info("Scraping both instances concurrently...")
        df_new, df_old = _scrape_with_interrupt_handling(
            config,
            profile_ids,
            profile_ids,
        )

    _finalize_output(df_old, df_new, output_path, cli_config.output_file.name, config)
    logger.info("Finished in %.2f seconds", time.time() - start)


if __name__ == "__main__":
    main()
