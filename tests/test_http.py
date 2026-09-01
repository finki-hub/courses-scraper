from pathlib import Path
from unittest.mock import Mock

import pytest
import requests
from requests.adapters import HTTPAdapter

from app.constants import COL_ID, selectors_new
from app.http import (
    REQUEST_TIMEOUT,
    EmptyReason,
    InstanceHttpConfig,
    PreflightError,
    PreflightFailureReason,
    ProfileEmpty,
    ProfileSuccess,
    ProfileTransportFailure,
    create_session,
    fetch_profile,
    preflight_instance,
)

FIXTURES = Path(__file__).parent / "fixtures"
BASE_URL = "https://courses.finki.ukim.mk"


def _config() -> InstanceHttpConfig:
    return InstanceHttpConfig(
        base_url=BASE_URL,
        cookie="secret-cookie",
        selectors=selectors_new,
        threads=2,
    )


def _response(
    status_code: int,
    body: str = "",
    *,
    location: str | None = None,
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = body.encode()
    response.url = f"{BASE_URL}/user/profile.php"
    if location is not None:
        response.headers["Location"] = location
    return response


def test_session_scopes_secure_cookie_and_identifies_scraper() -> None:
    # Given one Moodle instance configuration.
    config = _config()

    # When its HTTP session is created.
    with create_session(config) as session:
        exact_host = session.prepare_request(requests.Request("GET", f"{BASE_URL}/"))
        sibling_host = session.prepare_request(
            requests.Request("GET", "https://cas.courses.finki.ukim.mk/"),
        )
        other_instance = session.prepare_request(
            requests.Request("GET", "https://oldcourses.finki.ukim.mk/"),
        )
        insecure_host = session.prepare_request(
            requests.Request("GET", "http://courses.finki.ukim.mk/"),
        )

        # Then credentials stay on the exact HTTPS host.
        assert exact_host.headers["Cookie"] == "MoodleSession=secret-cookie"
        assert "Cookie" not in sibling_host.headers
        assert "Cookie" not in other_instance.headers
        assert "Cookie" not in insecure_host.headers
        assert session.headers["User-Agent"] == (
            "courses-scraper/0.1.0 "
            "(+https://github.com/finki-hub/courses-scraper; profile export)"
        )


def test_session_preserves_retryable_status_policy() -> None:
    # Given a configured instance session.
    with create_session(_config()) as session:
        # When its HTTPS transport policy is inspected.
        adapter = session.get_adapter(BASE_URL)
        assert isinstance(adapter, HTTPAdapter)
        retries = adapter.max_retries

        # Then the existing status retry contract is retained.
        assert retries.total == 5
        assert retries.status_forcelist == {429, 500, 502, 503, 504}
        assert retries.allowed_methods == frozenset({"HEAD", "GET", "OPTIONS"})


def test_preflight_disables_redirects_and_accepts_profile() -> None:
    # Given an authenticated response containing a real profile.
    session = Mock(spec=requests.Session)
    session.get.return_value = _response(
        200,
        (FIXTURES / "profile_new.html").read_text(encoding="utf-8"),
    )

    # When the instance preflight runs.
    preflight_instance(session, _config())

    # Then it probes the current profile without following auth redirects.
    session.get.assert_called_once_with(
        f"{BASE_URL}/user/profile.php",
        allow_redirects=False,
        timeout=REQUEST_TIMEOUT,
    )


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        (
            _response(302, location="/login/index.php"),
            PreflightFailureReason.LOGIN_REDIRECT,
        ),
        (_response(503), PreflightFailureReason.HTTP_STATUS),
        (
            _response(200, "<html><body></body></html>"),
            PreflightFailureReason.EMPTY_PROFILE,
        ),
    ],
)
def test_preflight_rejects_unauthenticated_or_unusable_responses(
    response: requests.Response,
    reason: PreflightFailureReason,
) -> None:
    # Given a response that cannot prove authenticated profile access.
    session = Mock(spec=requests.Session)
    session.get.return_value = response

    # When the instance preflight runs, then it fails closed with a typed reason.
    with pytest.raises(PreflightError) as caught:
        preflight_instance(session, _config())

    assert caught.value.reason is reason


def test_preflight_wraps_transport_failure() -> None:
    # Given an unavailable Moodle instance.
    session = Mock(spec=requests.Session)
    session.get.side_effect = requests.ConnectTimeout("offline")

    # When the instance preflight runs, then transport failure cannot become
    # an empty scrape.
    with pytest.raises(PreflightError) as caught:
        preflight_instance(session, _config())

    assert caught.value.reason is PreflightFailureReason.TRANSPORT


def test_fetch_profile_returns_success_with_local_id() -> None:
    # Given a successful profile response.
    session = Mock(spec=requests.Session)
    session.get.return_value = _response(
        200,
        (FIXTURES / "profile_new.html").read_text(encoding="utf-8"),
    )

    # When that profile is fetched.
    outcome = fetch_profile(session, 42, _config())

    # Then the parsed profile and instance-local ID are explicit success data.
    assert isinstance(outcome, ProfileSuccess)
    assert outcome.profile[COL_ID] == "42"
    assert outcome.profile["Name"] == "Ada Lovelace"


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        (_response(404), EmptyReason.HTTP_STATUS),
        (_response(200, "<html><body></body></html>"), EmptyReason.EMPTY_PROFILE),
    ],
)
def test_fetch_profile_distinguishes_empty_outcomes(
    response: requests.Response,
    reason: EmptyReason,
) -> None:
    # Given an HTTP response that has no exportable profile.
    session = Mock(spec=requests.Session)
    session.get.return_value = response

    # When the profile is fetched.
    outcome = fetch_profile(session, 42, _config())

    # Then it is an explicit empty outcome rather than success or transport failure.
    assert isinstance(outcome, ProfileEmpty)
    assert outcome.reason is reason


def test_fetch_profile_distinguishes_transport_failure() -> None:
    # Given a request that cannot reach Moodle.
    session = Mock(spec=requests.Session)
    session.get.side_effect = requests.ConnectionError("offline")

    # When the profile is fetched.
    outcome = fetch_profile(session, 42, _config())

    # Then the infrastructure failure is explicit.
    assert isinstance(outcome, ProfileTransportFailure)
    assert outcome.profile_id == 42
