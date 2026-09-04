from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from unittest.mock import Mock

import pytest

import app.__main__ as scraper
from app import cli
from app.auth import CasCredentials, InstanceCookies, ManualCookies
from app.cli import CliConfig, parse_cli


def _parse(
    argv: Sequence[str],
    *,
    environ: Mapping[str, str] | None = None,
    prompt_text: Callable[[str], str] | None = None,
    prompt_secret: Callable[[str], str] | None = None,
) -> CliConfig:
    if prompt_text is None:
        prompt_text = Mock(side_effect=AssertionError("unexpected text prompt"))
    if prompt_secret is None:
        prompt_secret = Mock(side_effect=AssertionError("unexpected secret prompt"))
    return parse_cli(
        argv,
        environ=environ or {},
        prompt_text=prompt_text,
        prompt_secret=prompt_secret,
    )


def test_parse_cli_returns_validated_values() -> None:
    # Given valid cookies and a positive maximum ID.
    argv = ["-c1", "new-cookie", "-c2", "old-cookie", "-m", "3", "-t", "4"]

    # When the command line is parsed.
    config = _parse(argv)

    # Then downstream execution receives complete typed values.
    assert config == CliConfig(
        authentication=ManualCookies(
            new=InstanceCookies(moodle_session="new-cookie"),
            old=InstanceCookies(moodle_session="old-cookie"),
        ),
        output_file=Path("output/profiles.csv"),
        threads=4,
        profile_ids=range(1, 4),
    )


def test_parse_cli_deduplicates_explicit_ids_stably() -> None:
    # Given repeated positive explicit IDs.
    argv = [
        "-c1",
        "new-cookie",
        "-c2",
        "old-cookie",
        "-i",
        "3",
        "1",
        "3",
        "2",
    ]

    # When the command line is parsed.
    config = _parse(argv)

    # Then first-seen order is retained without duplicates.
    assert config.profile_ids == (3, 1, 2)


@pytest.mark.parametrize(
    "numeric_arguments",
    [
        ["-m", "0"],
        ["-m", "-1"],
        ["-m", "1", "-t", "0"],
        ["-m", "1", "-t", "-1"],
        ["-i", "1", "0"],
        ["-i", "1", "-2"],
    ],
)
def test_parse_cli_rejects_nonpositive_execution_numbers(
    numeric_arguments: list[str],
) -> None:
    # Given a nonpositive thread, maximum, or explicit profile ID.
    argv = ["-c1", "new-cookie", "-c2", "old-cookie", *numeric_arguments]

    # When the command line is parsed, then execution is rejected.
    with pytest.raises(SystemExit) as caught:
        _parse(argv)

    assert caught.value.code == 2


@pytest.mark.parametrize(
    "output_name",
    [
        "../profiles.csv",
        "nested/profiles.csv",
        "nested\\profiles.csv",
        str(Path.home() / "profiles.csv"),
        "C:\\tmp\\profiles.csv",
        "CON",
        "con.csv",
        "profile.",
        "profile ",
        "profile*.csv",
        ".",
        "..",
    ],
)
def test_parse_cli_rejects_output_outside_output_directory(output_name: str) -> None:
    # Given an absolute path or a filename containing directory components.
    argv = [
        "-c1",
        "new-cookie",
        "-c2",
        "old-cookie",
        "-m",
        "1",
        "-o",
        output_name,
    ]

    # When the command line is parsed, then path escape is rejected.
    with pytest.raises(SystemExit) as caught:
        _parse(argv)

    assert caught.value.code == 2


def test_output_filename_rejects_windows_path_under_posix_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given the path semantics used by the Linux CI runner.
    monkeypatch.setattr(cli, "Path", PurePosixPath)

    # When a Windows-style nested path is parsed, then it is still rejected.
    with pytest.raises(argparse.ArgumentTypeError):
        cli._plain_output_filename("nested\\profiles.csv")


@pytest.mark.parametrize(
    "cookie",
    ["", " ", "two words", "line\nbreak", "value;other", "value,other", "nul\0byte"],
)
def test_parse_cli_rejects_unsafe_cookie_syntax(cookie: str) -> None:
    # Given a blank cookie or one unsafe for a Cookie header value.
    argv = ["-c1", cookie, "-c2", "old-cookie", "-m", "1"]

    # When the command line is parsed, then unsafe interpolation is rejected.
    with pytest.raises(SystemExit) as caught:
        _parse(argv)

    assert caught.value.code == 2


def test_parse_cli_prefers_explicit_cookies_over_environment() -> None:
    # Given distinct explicit and environment cookie values.
    argv = ["-c1", "flag-new", "-c2", "flag-old", "-m", "1"]
    environ = {
        "COURSES_COOKIE_NEW": "env-new",
        "COURSES_COOKIE_OLD": "env-old",
    }

    # When the command line is parsed.
    config = _parse(argv, environ=environ)

    # Then explicit flags win independently for both instances.
    assert config.authentication == ManualCookies(
        new=InstanceCookies(moodle_session="flag-new"),
        old=InstanceCookies(moodle_session="flag-old"),
    )


def test_parse_cli_uses_environment_before_prompting() -> None:
    # Given valid environment cookies and no cookie flags.
    environ = {
        "COURSES_COOKIE_NEW": "env-new",
        "COURSES_COOKIE_OLD": "env-old",
    }

    # When the command line is parsed.
    config = _parse(["-m", "1"], environ=environ)

    # Then environment values satisfy both cookie inputs.
    assert config.authentication == ManualCookies(
        new=InstanceCookies(moodle_session="env-new"),
        old=InstanceCookies(moodle_session="env-old"),
    )


def test_parse_cli_prompts_for_each_missing_cookie() -> None:
    # Given no cookie flags or environment variables.
    supplied = iter(["prompt-new", "prompt-old"])
    prompt_secret = Mock(side_effect=lambda _prompt: next(supplied))

    # When the command line is parsed.
    config = _parse(["-m", "1"], prompt_secret=prompt_secret)

    # Then hidden-input prompting supplies each cookie once.
    assert config.authentication == ManualCookies(
        new=InstanceCookies(moodle_session="prompt-new"),
        old=InstanceCookies(moodle_session="prompt-old"),
    )
    assert prompt_secret.call_count == 2


def test_parse_cli_uses_cas_credentials_without_cookie_prompts() -> None:
    # Given CAS mode credentials supplied without command-line secrets.
    environ = {
        "COURSES_CAS_USERNAME": "student",
        "COURSES_CAS_PASSWORD": "secret",
    }

    # When the command line is parsed in CAS mode.
    config = _parse(["--cas", "-m", "1"], environ=environ)

    # Then one credential pair replaces both manual cookie inputs.
    assert config.authentication == CasCredentials(
        username="student",
        password=environ["COURSES_CAS_PASSWORD"],
    )


def test_parse_cli_prompts_once_for_missing_cas_credentials() -> None:
    # Given CAS mode without credential flags or environment variables.
    prompt_text = Mock(return_value="student")
    prompt_secret = Mock(return_value="secret")

    # When the command line is parsed in CAS mode.
    config = _parse(
        ["--cas", "-m", "1"],
        prompt_text=prompt_text,
        prompt_secret=prompt_secret,
    )

    # Then one username prompt and one hidden password prompt supply both services.
    assert config.authentication == CasCredentials(
        username="student",
        password=prompt_secret.return_value,
    )
    prompt_text.assert_called_once()
    prompt_secret.assert_called_once()


def test_parse_cli_rejects_manual_cookie_flags_in_cas_mode() -> None:
    # Given mutually conflicting authentication modes.
    environ = {
        "COURSES_CAS_USERNAME": "student",
        "COURSES_CAS_PASSWORD": "secret",
    }

    # When the command line is parsed, then the ambiguity is rejected.
    with pytest.raises(SystemExit) as caught:
        _parse(["--cas", "-c1", "cookie", "-m", "1"], environ=environ)

    assert caught.value.code == 2


def test_parse_cli_rejects_blank_explicit_cas_username() -> None:
    # Given a blank explicit username and a valid fallback environment value.
    environ = {
        "COURSES_CAS_USERNAME": "fallback-student",
        "COURSES_CAS_PASSWORD": "secret",
    }

    # When the explicit value is parsed, then it is rejected rather than bypassed.
    with pytest.raises(SystemExit) as caught:
        _parse(
            ["--cas", "--cas-username", "", "-m", "1"],
            environ=environ,
        )

    assert caught.value.code == 2


@pytest.mark.parametrize(
    ("environ", "missing_flag", "missing_variable"),
    [
        ({}, "-c1", "COURSES_COOKIE_NEW"),
        ({"COURSES_COOKIE_NEW": "new-cookie"}, "-c2", "COURSES_COOKIE_OLD"),
    ],
)
def test_parse_cli_reports_actionable_missing_cookie(
    environ: Mapping[str, str],
    missing_flag: str,
    missing_variable: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Given a missing cookie and unavailable secret input.
    prompt_secret = Mock(side_effect=EOFError)

    # When the command line is parsed, then it exits with source guidance.
    with pytest.raises(SystemExit) as caught:
        _parse(["-m", "1"], environ=environ, prompt_secret=prompt_secret)

    assert caught.value.code == 2
    error = capsys.readouterr().err
    assert missing_flag in error
    assert missing_variable in error


def test_main_rejects_invalid_cli_before_network_or_output_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given an invalid thread count with otherwise complete command-line input.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["courses-scraper", "-c1", "new", "-c2", "old", "-m", "1", "-t", "0"],
    )
    create_session = Mock(side_effect=AssertionError("network side effect"))
    monkeypatch.setattr(scraper, "create_session", create_session)

    # When main starts, then parsing exits before network or filesystem output.
    with pytest.raises(SystemExit) as caught:
        scraper.main()

    assert caught.value.code == 2
    assert not (tmp_path / "output").exists()
    create_session.assert_not_called()
