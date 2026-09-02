from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests
from requests.adapters import HTTPAdapter

from app.constants import COL_ID, selectors_new
from app.http import (
    REQUEST_TIMEOUT,
    TRANSPORT_FAILURE_RATE_THRESHOLD,
    EmptyReason,
    InstanceHttpConfig,
    PreflightError,
    PreflightFailureReason,
    ProfileEmpty,
    ProfileFailureReason,
    ProfileRequestError,
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
        assert retries.raise_on_status is False
        assert TRANSPORT_FAILURE_RATE_THRESHOLD == 0.5


def test_session_rejects_insecure_instance_url() -> None:
    # Given an instance URL that would send scraper traffic over plain HTTP.
    config = replace(_config(), base_url="http://courses.finki.ukim.mk")

    # When the session is created, then insecure transport is rejected.
    with pytest.raises(ValueError, match="HTTPS"):
        create_session(config)


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
        (
            _response(
                200,
                '<form id="login"><input name="username">'
                '<input name="password"></form>',
            ),
            PreflightFailureReason.LOGIN_RESPONSE,
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


@pytest.mark.parametrize("status_code", [401, 403, 503])
def test_fetch_profile_does_not_treat_http_failures_as_empty(
    status_code: int,
) -> None:
    # Given an authenticated scrape request that receives a fatal HTTP response.
    session = Mock(spec=requests.Session)
    session.get.return_value = _response(status_code)

    # When the response is classified, then it cannot become completed empty work.
    with pytest.raises(ProfileRequestError) as caught:
        fetch_profile(session, 42, _config())

    assert caught.value.profile_id == 42
    assert caught.value.reason is ProfileFailureReason.HTTP_STATUS
    assert caught.value.status_code == status_code


def test_fetch_profile_does_not_follow_or_complete_redirects() -> None:
    # Given a profile request redirected to the Moodle login page.
    session = Mock(spec=requests.Session)
    session.get.return_value = _response(302, location="/login/index.php")

    # When the profile is fetched, then redirect handling remains explicit.
    with pytest.raises(ProfileRequestError) as caught:
        fetch_profile(session, 42, _config())

    session.get.assert_called_once_with(
        f"{BASE_URL}/user/profile.php?id=42&showallcourses=1",
        allow_redirects=False,
        timeout=REQUEST_TIMEOUT,
    )
    assert caught.value.profile_id == 42
    assert caught.value.reason is ProfileFailureReason.REDIRECT
    assert caught.value.status_code == 302


def test_fetch_profile_does_not_complete_login_page_response() -> None:
    # Given a successful profile URL that renders Moodle's login form.
    session = Mock(spec=requests.Session)
    response = _response(
        200,
        '<form id="login"><input name="username"><input name="password"></form>',
    )
    session.get.return_value = response

    # When the response is classified, then it cannot become completed empty work.
    with pytest.raises(ProfileRequestError) as caught:
        fetch_profile(session, 42, _config())

    assert caught.value.profile_id == 42
    assert caught.value.reason is ProfileFailureReason.LOGIN_RESPONSE
    assert caught.value.status_code == 200


def test_fetch_profile_preserves_parse_errors_as_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = Mock(spec=requests.Session)
    session.get.return_value = _response(200, "<html></html>")
    monkeypatch.setattr(
        "app.http.parse_profile_html",
        Mock(side_effect=TypeError("invalid profile markup")),
    )

    outcome = fetch_profile(session, 42, _config())

    assert isinstance(outcome, ProfileEmpty)
    assert outcome.reason is EmptyReason.PARSE_ERROR


def test_fetch_profile_distinguishes_transport_failure() -> None:
    # Given a request that cannot reach Moodle.
    session = Mock(spec=requests.Session)
    session.get.side_effect = requests.ConnectionError("offline")

    # When the profile is fetched.
    outcome = fetch_profile(session, 42, _config())

    # Then the infrastructure failure is explicit.
    assert isinstance(outcome, ProfileTransportFailure)
    assert outcome.profile_id == 42
