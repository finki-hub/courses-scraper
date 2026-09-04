from unittest.mock import MagicMock, call

import pytest
import requests

from app import auth

BASE_URL = "https://courses.finki.ukim.mk"


def _response(
    body: str = "",
    *,
    status_code: int = 200,
    location: str | None = None,
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = body.encode()
    response.raw = MagicMock()
    response.url = BASE_URL
    if location is not None:
        response.headers["Location"] = location
    return response


def _session_with_cookies() -> MagicMock:
    session = MagicMock(spec=requests.Session)
    session.__enter__.return_value = session
    cookie_session = requests.Session()
    cookie_session.cookies.set(
        "MoodleSession", "moodle-cookie", domain="courses.finki.ukim.mk"
    )
    cookie_session.cookies.set(
        "SRVNAME", "server-cookie", domain="courses.finki.ukim.mk"
    )
    session.cookies = cookie_session.cookies
    session.prepare_request.side_effect = cookie_session.prepare_request
    return session


def test_authenticate_instance_validates_each_same_origin_redirect() -> None:
    session = _session_with_cookies()
    ticket_url = f"{BASE_URL}/login/index.php?ticket=service-ticket"
    finish_url = f"{BASE_URL}/auth/finish"
    session.get.side_effect = [
        _response(
            '<form><input type="hidden" name="execution" value="token">'
            '<input type="hidden" name="_eventId" value="submit"></form>',
        ),
        _response(status_code=303, location="/auth/finish"),
        _response(),
    ]
    session.post.return_value = _response(status_code=302, location=ticket_url)

    auth.authenticate_instance(
        auth.CasCredentials("student", "credential-value"),
        BASE_URL,
        session_factory=lambda: session,
    )

    assert session.get.call_args_list == [
        call(
            auth.CAS_LOGIN_URL
            + "?service=https%3A%2F%2Fcourses.finki.ukim.mk%2Flogin%2Findex.php",
            allow_redirects=False,
            timeout=auth.REQUEST_TIMEOUT,
        ),
        call(ticket_url, allow_redirects=False, timeout=auth.REQUEST_TIMEOUT),
        call(finish_url, allow_redirects=False, timeout=auth.REQUEST_TIMEOUT),
    ]


@pytest.mark.parametrize(
    ("status_code", "location"),
    [
        (302, "https://example.com/collect"),
        (302, "http://courses.finki.ukim.mk/collect"),
        (302, "https://courses.finki.ukim.mk:444/collect"),
        (307, f"{BASE_URL}/collect"),
        (308, f"{BASE_URL}/collect"),
    ],
)
def test_authenticate_instance_rejects_unsafe_second_redirect(
    status_code: int,
    location: str,
) -> None:
    session = _session_with_cookies()
    ticket_url = f"{BASE_URL}/login/index.php?ticket=service-ticket"
    session.get.side_effect = [
        _response(
            '<form><input type="hidden" name="execution" value="token">'
            '<input type="hidden" name="_eventId" value="submit"></form>',
        ),
        _response(status_code=status_code, location=location),
    ]
    session.post.return_value = _response(status_code=302, location=ticket_url)

    with pytest.raises(auth.CasAuthenticationError) as caught:
        auth.authenticate_instance(
            auth.CasCredentials("student", "credential-value"),
            BASE_URL,
            session_factory=lambda: session,
        )

    assert caught.value.reason is auth.CasAuthenticationFailure.UNSAFE_REDIRECT
    assert session.get.call_count == 2


def test_authenticate_instance_rejects_initial_cas_redirect() -> None:
    session = _session_with_cookies()
    session.get.return_value = _response(
        status_code=302, location="https://example.com"
    )

    with pytest.raises(auth.CasAuthenticationError) as caught:
        auth.authenticate_instance(
            auth.CasCredentials("student", "credential-value"),
            BASE_URL,
            session_factory=lambda: session,
        )

    assert caught.value.reason is auth.CasAuthenticationFailure.UNSAFE_REDIRECT
    session.post.assert_not_called()


def test_authenticate_instance_rejects_redirect_loop() -> None:
    session = _session_with_cookies()
    ticket_url = f"{BASE_URL}/login/index.php?ticket=service-ticket"
    session.get.side_effect = [
        _response(
            '<form><input type="hidden" name="execution" value="token">'
            '<input type="hidden" name="_eventId" value="submit"></form>',
        ),
        *[
            _response(status_code=302, location="/loop")
            for _ in range(auth.MAX_SERVICE_REDIRECTS + 1)
        ],
    ]
    session.post.return_value = _response(status_code=302, location=ticket_url)

    with pytest.raises(auth.CasAuthenticationError) as caught:
        auth.authenticate_instance(
            auth.CasCredentials("student", "credential-value"),
            BASE_URL,
            session_factory=lambda: session,
        )

    assert caught.value.reason is auth.CasAuthenticationFailure.UNSAFE_REDIRECT
    assert session.get.call_count == auth.MAX_SERVICE_REDIRECTS + 2
