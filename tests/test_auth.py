from unittest.mock import MagicMock, call

import pytest
import requests

from app import auth

BASE_URL = "https://courses.finki.ukim.mk"
CREDENTIAL_VALUE = "credential-value"


def _response(
    body: str = "",
    *,
    status_code: int = 200,
    location: str | None = None,
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = body.encode()
    response.url = BASE_URL
    if location is not None:
        response.headers["Location"] = location
    return response


def test_authentication_types_are_available() -> None:
    # Given the authentication adapter is imported.
    # When its public credential and cookie types are inspected.
    exports = (
        getattr(auth, "CasCredentials", None),
        getattr(auth, "InstanceCookies", None),
        getattr(auth, "ManualCookies", None),
    )

    # Then callers can represent both credential input and per-host cookies.
    assert all(export is not None for export in exports)


def test_authenticate_instance_preserves_hidden_fields_and_service_cookies() -> None:
    # Given a CAS form and a redirect chain that produced both Moodle cookies.
    session = MagicMock(spec=requests.Session)
    session.__enter__.return_value = session
    service_url = f"{BASE_URL}/login/index.php?ticket=service-ticket"
    session.get.side_effect = [
        _response(
            '<form><input type="hidden" name="execution" value="token">'
            '<input type="hidden" name="_eventId" value="submit"></form>',
        ),
        _response(),
    ]
    session.post.return_value = _response(status_code=302, location=service_url)
    cookie_session = requests.Session()
    cookie_session.cookies.set(
        "MoodleSession",
        "moodle-cookie",
        domain="courses.finki.ukim.mk",
        path="/",
        secure=True,
    )
    cookie_session.cookies.set(
        "SRVNAME",
        "server-cookie",
        domain="courses.finki.ukim.mk",
        path="/",
        secure=True,
    )
    session.cookies = cookie_session.cookies
    session.prepare_request.side_effect = cookie_session.prepare_request
    authenticate = getattr(auth, "authenticate_instance", None)

    # When the credentials are authenticated for the Courses instance.
    assert callable(authenticate)
    cookies = authenticate(
        auth.CasCredentials(username="student", password=CREDENTIAL_VALUE),
        BASE_URL,
        session_factory=lambda: session,
    )

    # Then the CAS fields are submitted and both host cookies are retained.
    assert cookies == auth.InstanceCookies(
        moodle_session="moodle-cookie",
        server_name="server-cookie",
    )
    post = session.post.call_args
    assert post.kwargs["data"] == [
        ("execution", "token"),
        ("_eventId", "submit"),
        ("username", "student"),
        ("password", CREDENTIAL_VALUE),
        ("submit", "LOGIN"),
    ]
    assert post.kwargs["allow_redirects"] is False
    assert session.get.call_args_list[-1] == call(
        service_url,
        allow_redirects=True,
        timeout=auth.REQUEST_TIMEOUT,
    )


def test_authenticate_instance_rejects_incomplete_service_cookie_set() -> None:
    # Given CAS returns only a MoodleSession cookie without the routing cookie.
    session = MagicMock(spec=requests.Session)
    session.__enter__.return_value = session
    session.get.return_value = _response()
    session.post.return_value = _response()
    cookie_session = requests.Session()
    cookie_session.cookies.set(
        "MoodleSession",
        "moodle-cookie",
        domain="courses.finki.ukim.mk",
        path="/",
        secure=True,
    )
    session.cookies = cookie_session.cookies
    session.prepare_request.side_effect = cookie_session.prepare_request
    authenticate = getattr(auth, "authenticate_instance", None)
    error_type = getattr(auth, "CasAuthenticationError", RuntimeError)

    # When authentication completes without the expected complete cookie set.
    assert callable(authenticate)
    with pytest.raises(error_type):
        authenticate(
            auth.CasCredentials(username="student", password=CREDENTIAL_VALUE),
            BASE_URL,
            session_factory=lambda: session,
        )


def test_authentication_error_supports_normal_traceback_assignment() -> None:
    # Given an authentication error that may cross exception boundaries.
    error = auth.CasAuthenticationError(
        service_url=f"{BASE_URL}/login/index.php",
        reason=auth.CasAuthenticationFailure.MISSING_COOKIES,
    )

    # When Python exception machinery assigns traceback state.
    error.__traceback__ = None

    # Then the exception remains usable by context managers and test runners.
    assert error.__traceback__ is None


def test_authenticate_instance_rejects_credential_replaying_redirect() -> None:
    # Given CAS responds with a redirect that would preserve the credential POST.
    session = MagicMock(spec=requests.Session)
    session.__enter__.return_value = session
    session.get.return_value = _response()
    session.post.return_value = _response(
        status_code=307,
        location="https://example.com/collect",
    )
    session.cookies = requests.cookies.RequestsCookieJar()
    authenticate = auth.authenticate_instance

    # When the redirect is evaluated, then credentials are never replayed.
    with pytest.raises(auth.CasAuthenticationError) as caught:
        authenticate(
            auth.CasCredentials("student", CREDENTIAL_VALUE),
            BASE_URL,
            session_factory=lambda: session,
        )

    assert caught.value.reason is auth.CasAuthenticationFailure.UNSAFE_REDIRECT


def test_cas_credentials_hide_password_from_representations() -> None:
    # Given a credential object used by CLI configuration and diagnostics.
    credentials = auth.CasCredentials("student", CREDENTIAL_VALUE)

    # When it is represented for debugging, then the password is absent.
    assert CREDENTIAL_VALUE not in repr(credentials)
