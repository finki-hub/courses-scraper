from __future__ import annotations

import argparse
import getpass
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

OUTPUT_DIRECTORY: Final = Path("output")


@dataclass(frozen=True, slots=True)
class CliConfig:
    cookie_new: str
    cookie_old: str
    output_file: Path
    threads: int
    profile_ids: range | tuple[int, ...]


class _Arguments(argparse.Namespace):
    c1: str | None
    c2: str | None
    o: Path
    t: int
    i: list[int] | None
    m: int | None


@dataclass(frozen=True, slots=True)
class _CookieInput:
    explicit: str | None
    environment_name: str
    flag: str
    prompt: str


def _positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        message = "must be a positive integer"
        raise argparse.ArgumentTypeError(message)
    return value


def _plain_output_filename(raw: str) -> Path:
    candidate = Path(raw)
    if (
        candidate.is_absolute()
        or candidate.parent != Path()
        or candidate.name in {"", ".", ".."}
    ):
        message = "must be a plain filename inside output/"
        raise argparse.ArgumentTypeError(message)
    return candidate


def parse_cli(
    argv: Sequence[str] | None = None,
    environ: Mapping[str, str] | None = None,
    prompt_secret: Callable[[str], str] = getpass.getpass,
) -> CliConfig:
    parser = argparse.ArgumentParser(
        description="Scrape Courses profiles from two instances",
    )
    parser.add_argument("-c1", help="New Courses instance session cookie")
    parser.add_argument("-c2", help="Old Courses instance session cookie")
    parser.add_argument(
        "-o",
        type=_plain_output_filename,
        default=Path("profiles.csv"),
        help="Output file",
    )
    parser.add_argument(
        "-t",
        type=_positive_int,
        default=10,
        help="How many threads to use",
    )

    id_group = parser.add_mutually_exclusive_group(required=True)
    id_group.add_argument(
        "-i",
        type=_positive_int,
        nargs="+",
        help="Profile IDs to scrape",
    )
    id_group.add_argument("-m", type=_positive_int, help="Highest ID")

    arguments = parser.parse_args(argv, namespace=_Arguments())
    environment = os.environ if environ is None else environ

    def resolve_cookie(cookie_input: _CookieInput) -> str:
        value = cookie_input.explicit
        if value is None:
            value = environment.get(cookie_input.environment_name)
        if value is None:
            try:
                value = prompt_secret(cookie_input.prompt)
            except EOFError:
                parser.error(
                    f"missing cookie: supply {cookie_input.flag}, set "
                    f"{cookie_input.environment_name}, or enter it at "
                    "the hidden prompt",
                )
        if not value or any(
            not character.isprintable() or character.isspace() or character in ";,"
            for character in value
        ):
            parser.error(
                f"invalid cookie from {cookie_input.flag} or "
                f"{cookie_input.environment_name}: must be nonblank and contain no "
                "control characters, whitespace, semicolons, or commas",
            )
        return value

    cookie_new = resolve_cookie(
        _CookieInput(
            explicit=arguments.c1,
            environment_name="COURSES_COOKIE_NEW",
            flag="-c1",
            prompt="New Courses MoodleSession cookie: ",
        ),
    )
    cookie_old = resolve_cookie(
        _CookieInput(
            explicit=arguments.c2,
            environment_name="COURSES_COOKIE_OLD",
            flag="-c2",
            prompt="Old Courses MoodleSession cookie: ",
        ),
    )

    profile_ids: range | tuple[int, ...]
    if arguments.i is not None:
        profile_ids = tuple(dict.fromkeys(arguments.i))
    else:
        if arguments.m is None:
            parser.error("one of -i or -m is required")
        profile_ids = range(1, arguments.m + 1)

    return CliConfig(
        cookie_new=cookie_new,
        cookie_old=cookie_old,
        output_file=OUTPUT_DIRECTORY / arguments.o,
        threads=arguments.t,
        profile_ids=profile_ids,
    )
