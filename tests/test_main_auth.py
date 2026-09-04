from pathlib import Path
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

import app.__main__ as scraper
from app.auth import (
    CasAuthenticationError,
    CasAuthenticationFailure,
    CasCredentials,
    InstanceCookies,
    ManualCookies,
)
from app.cli import CliConfig
from app.http import PreflightError, PreflightFailureReason

CREDENTIAL_VALUE = "credential-value"


def test_main_authenticates_cas_credentials_for_each_instance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    credentials = CasCredentials(username="student", password=CREDENTIAL_VALUE)
    monkeypatch.setattr(
        scraper,
        "parse_cli",
        Mock(
            return_value=CliConfig(
                authentication=credentials,
                output_file=Path("output/out.csv"),
                threads=1,
                profile_ids=(1,),
            ),
        ),
    )
    authenticate = Mock(
        side_effect=[
            InstanceCookies("new-session", "new-server"),
            InstanceCookies("old-session", "old-server"),
        ],
    )
    monkeypatch.setattr(scraper, "authenticate_instance", authenticate)
    session = MagicMock()
    session.__enter__.return_value = session
    monkeypatch.setattr(scraper, "create_session", Mock(return_value=session))
    preflight = Mock()
    monkeypatch.setattr(scraper, "preflight_instance", preflight)
    scrape = Mock(return_value=(pd.DataFrame(), pd.DataFrame()))
    monkeypatch.setattr(scraper, "_scrape_with_interrupt_handling", scrape)
    monkeypatch.setattr(scraper, "_finalize_output", Mock())

    scraper.main()

    assert authenticate.call_args_list == [
        ((credentials, "https://courses.finki.ukim.mk"),),
        ((credentials, "https://oldcourses.finki.ukim.mk"),),
    ]
    runtime_config = scrape.call_args.args[0]
    assert runtime_config.http_new.cookies == InstanceCookies(
        "new-session",
        "new-server",
    )
    assert runtime_config.http_old.cookies == InstanceCookies(
        "old-session",
        "old-server",
    )
    preflight.assert_not_called()


def test_main_stops_before_output_when_cas_authentication_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    credentials = CasCredentials(username="student", password=CREDENTIAL_VALUE)
    monkeypatch.setattr(
        scraper,
        "parse_cli",
        Mock(
            return_value=CliConfig(
                authentication=credentials,
                output_file=Path("output/out.csv"),
                threads=1,
                profile_ids=(1,),
            ),
        ),
    )
    monkeypatch.setattr(
        scraper,
        "authenticate_instance",
        Mock(
            side_effect=CasAuthenticationError(
                service_url="https://courses.finki.ukim.mk/login/index.php",
                reason=CasAuthenticationFailure.MISSING_COOKIES,
                missing_cookies=("MoodleSession", "SRVNAME"),
            ),
        ),
    )
    scrape = Mock(side_effect=AssertionError("unexpected scrape"))
    monkeypatch.setattr(scraper, "_scrape_with_interrupt_handling", scrape)

    with pytest.raises(SystemExit) as caught:
        scraper.main()

    assert caught.value.code == 2
    assert not (tmp_path / "output").exists()
    scrape.assert_not_called()


def test_main_does_not_create_output_before_preflight_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        scraper,
        "parse_cli",
        Mock(
            return_value=CliConfig(
                authentication=ManualCookies(
                    new=InstanceCookies("new-session"),
                    old=InstanceCookies("old-session"),
                ),
                output_file=Path("output/out.csv"),
                threads=1,
                profile_ids=(1,),
            ),
        ),
    )
    monkeypatch.setattr(
        scraper,
        "_scrape_with_interrupt_handling",
        Mock(
            side_effect=PreflightError(
                base_url="https://courses.finki.ukim.mk",
                reason=PreflightFailureReason.LOGIN_RESPONSE,
            ),
        ),
    )

    with pytest.raises(PreflightError):
        scraper.main()

    assert not (tmp_path / "output").exists()
