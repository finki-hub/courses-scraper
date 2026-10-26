import logging
import os
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, NoReturn, assert_never

import pandas as pd

from app.auth import (
    CasAuthenticationError,
    CasCredentials,
    InstanceCookies,
    ManualCookies,
    authenticate_instance,
)
from app.checkpoints import (
    CheckpointPaths,
    CheckpointSnapshot,
    InstanceCheckpoint,
)
from app.checkpoints import clear as clear_checkpoints
from app.checkpoints import load as load_checkpoint
from app.checkpoints import save as save_checkpoint
from app.cli import CliConfig, parse_cli
from app.constants import (
    COL_ID,
    FIRST_UNMIGRATED_OLD_ID,
    base_urls,
    selectors_new,
    selectors_old,
)
from app.coordinator import CoordinatorPlan
from app.coordinator import run as run_coordinator
from app.export import export_profiles
from app.historical import (
    HistoricalSnapshot,
    load_historical_snapshot,
    merge_historical_snapshot,
)
from app.http import (
    InstanceHttpConfig,
    create_session,
    fetch_profile,
    preflight_instance,
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
    http_old: InstanceHttpConfig | None
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
            profile_ids_old=[
                profile_id
                for profile_id in profile_ids_old
                if profile_id < FIRST_UNMIGRATED_OLD_ID
            ],
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
    old_ids = profile_ids if config.http_old is not None else ()
    if snapshot is None:
        return _scrape_with_interrupt_handling(
            config,
            profile_ids,
            old_ids,
        )
    if config.http_old is None and (
        not snapshot.old.frame.empty or snapshot.old.completed_ids
    ):
        raise ValueError(
            "New-only checkpoint must not contain old profiles or completed IDs"
        )
    remaining_new = [
        profile_id
        for profile_id in profile_ids
        if profile_id not in snapshot.new.completed_ids
    ]
    remaining_old = [
        profile_id
        for profile_id in old_ids
        if profile_id < FIRST_UNMIGRATED_OLD_ID
        and profile_id not in snapshot.old.completed_ids
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

    historical = _load_history(cli_config)
    cookies_new, cookies_old, verify_all_instances = _authenticate(cli_config)
    config = _build_config(cli_config, cookies_new, cookies_old, historical)
    _run_and_export(cli_config, config, historical, verify_all_instances)
    logger.info("Finished in %.2f seconds", time.time() - start)


def _load_history(cli_config: CliConfig) -> HistoricalSnapshot | None:
    if cli_config.instances == "both":
        if cli_config.historical is not None:
            raise ValueError("--historical requires --instances new")
        return None
    if cli_config.historical is None:
        raise ValueError("--instances new requires --historical")
    source = cli_config.historical.resolve()
    output = cli_config.output_file.resolve()
    if source == output:
        raise ValueError("Historical source must not be the output destination")
    checkpoint_root = (
        cli_config.output_file.parent / ".checkpoints-new-only"
    ).resolve()
    if source.is_relative_to(checkpoint_root):
        raise ValueError("Historical source must not be inside new-only checkpoints")
    snapshot = load_historical_snapshot(source)
    logger.info(
        "New-only mode: reusing historical old data from %s (SHA256 %s)",
        snapshot.path,
        snapshot.sha256,
    )
    return snapshot


def _authenticate(
    cli_config: CliConfig,
) -> tuple[InstanceCookies, InstanceCookies | None, bool]:

    match cli_config.authentication:
        case ManualCookies(new=cookies_new, old=cookies_old):
            verify_all_instances = False
        case CasCredentials() as credentials:
            logger.info(
                "Authenticating %s Courses instance(s) through CAS...",
                cli_config.instances,
            )
            try:
                cookies_new = authenticate_instance(credentials, base_urls["new"])
                cookies_old = (
                    authenticate_instance(credentials, base_urls["old"])
                    if cli_config.instances == "both"
                    else None
                )
            except CasAuthenticationError as error:
                # Log only the safe public error, never its credential-bearing chain.
                safe_error = RuntimeError(str(error))
                logger.exception(
                    "CAS authentication failed",
                    exc_info=(RuntimeError, safe_error, None),
                )
                raise SystemExit(2) from error
            verify_all_instances = True
        case unreachable:
            assert_never(unreachable)

    if cli_config.instances == "both" and cookies_old is None:
        raise ValueError("Old cookies required for both instances")
    return cookies_new, cookies_old, verify_all_instances


def _build_config(
    cli_config: CliConfig,
    cookies_new: InstanceCookies,
    cookies_old: InstanceCookies | None,
    historical: HistoricalSnapshot | None,
) -> ScrapeConfig:
    output_path = cli_config.output_file.parent
    if historical is not None:
        output_path = output_path / ".checkpoints-new-only" / historical.sha256

    http_old = None
    if historical is None:
        if cookies_old is None:
            raise ValueError("Old cookies required for both instances")
        http_old = InstanceHttpConfig(
            base_url=base_urls["old"],
            cookies=cookies_old,
            selectors=selectors_old,
            threads=cli_config.threads,
        )
    config = ScrapeConfig(
        http_new=InstanceHttpConfig(
            base_url=base_urls["new"],
            cookies=cookies_new,
            selectors=selectors_new,
            threads=cli_config.threads,
        ),
        http_old=http_old,
        checkpoint_new=output_path / "checkpoint_new.csv",
        checkpoint_old=output_path / "checkpoint_old.csv",
    )
    return config


def _run_and_export(
    cli_config: CliConfig,
    config: ScrapeConfig,
    historical: HistoricalSnapshot | None,
    verify_all_instances: bool,
) -> None:
    profile_ids = cli_config.profile_ids

    checkpoint_paths = _checkpoint_paths(config)
    has_checkpoint = (
        checkpoint_paths.manifest.exists()
        or checkpoint_paths.legacy_new.exists()
        or checkpoint_paths.legacy_old.exists()
    )
    if verify_all_instances and has_checkpoint:
        for http_config in (config.http_new, config.http_old):
            if http_config is None:
                continue
            with create_session(http_config) as session:
                preflight_instance(session, http_config)

    if has_checkpoint:
        df_new, df_old = _resume_from_checkpoints(config, profile_ids)
    else:
        logger.info("Scraping %s instance(s)...", cli_config.instances)
        df_new, df_old = _scrape_with_interrupt_handling(
            config,
            profile_ids,
            profile_ids if config.http_old is not None else (),
        )

    if historical is None:
        _finalize_output(
            df_old,
            df_new,
            cli_config.output_file.parent,
            cli_config.output_file.name,
            config,
        )
        return
    if df_new.empty:
        raise ValueError("New-only scrape returned no fresh profiles")
    combined = merge_historical_snapshot(historical, df_new)
    export_profiles(combined, cli_config.output_file)
    clear_checkpoints(_checkpoint_paths(config))
    logger.info(
        "Written %d profiles to %s; historical old data reused",
        len(combined),
        cli_config.output_file,
    )


if __name__ == "__main__":
    main()
