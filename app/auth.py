"""CAS authentication for Moodle instances."""

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum, unique
from http import HTTPStatus
from http.cookies import SimpleCookie
from typing import Final
from urllib.parse import urlencode, urljoin, urlsplit

import requests
from bs4 import BeautifulSoup

CAS_LOGIN_URL: Final = "https://cas.finki.ukim.mk/cas/login"
REQUEST_TIMEOUT: Final = (5, 15)
MAX_SERVICE_REDIRECTS: Final = 5

__all__ = (
    "CasAuthenticationError",
    "CasAuthenticationFailure",
    "CasCredentials",
    "InstanceCookies",
    "ManualCookies",
    "authenticate_instance",
)


@dataclass(frozen=True, slots=True)
class CasCredentials:
    username: str
    password: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class InstanceCookies:
    moodle_session: str = field(repr=False)
    server_name: str | None = field(default=None, repr=False)

    def as_header(self) -> str:
        parts = [f"MoodleSession={self.moodle_session}"]
        if self.server_name is not None:
            parts.append(f"SRVNAME={self.server_name}")
        return "; ".join(parts)


@dataclass(frozen=True, slots=True)
class ManualCookies:
    new: InstanceCookies
    old: InstanceCookies


@unique
class CasAuthenticationFailure(StrEnum):
    REQUEST = "request_failed"
    INVALID_LOGIN_FORM = "invalid_login_form"
    MISSING_COOKIES = "missing_service_cookies"
    UNSAFE_REDIRECT = "unsafe_redirect"


class CasAuthenticationError(Exception):
    def __init__(
        self,
        service_url: str,
        reason: CasAuthenticationFailure,
        missing_cookies: tuple[str, ...] = (),
    ) -> None:
        super().__init__(service_url, reason, missing_cookies)
        self.service_url = service_url
        self.reason = reason
        self.missing_cookies = missing_cookies

    def __str__(self) -> str:
        missing = (
            "" if not self.missing_cookies else f": {', '.join(self.missing_cookies)}"
        )
        return (
            f"CAS authentication failed for {self.service_url}: "
            f"{self.reason.value}{missing}"
        )


def _unsafe_redirect(service_url: str) -> CasAuthenticationError:
    return CasAuthenticationError(
        service_url=service_url,
        reason=CasAuthenticationFailure.UNSAFE_REDIRECT,
    )


def _is_exact_https_origin(url: str, expected_url: str) -> bool:
    try:
        target = urlsplit(url)
        expected = urlsplit(expected_url)
        target_port = target.port or 443
        expected_port = expected.port or 443
    except ValueError:
        return False
    return (
        target.scheme == "https"
        and target.hostname == expected.hostname
        and target_port == expected_port
        and target.username is None
        and target.password is None
    )


def _login_form_data(
    soup: BeautifulSoup,
    service_url: str,
) -> list[tuple[str, str]]:
    candidates: list[list[tuple[str, str]]] = []
    for form in soup.select("form"):
        fields: list[tuple[str, str]] = []
        for field_element in form.select('input[type="hidden"]'):
            name = field_element.get("name")
            value = field_element.get("value", "")
            if isinstance(name, str):
                fields.append((name, value if isinstance(value, str) else ""))
        names = [name for name, _ in fields]
        if names.count("execution") == 1 and names.count("_eventId") == 1:
            candidates.append(fields)
    if len(candidates) != 1:
        raise CasAuthenticationError(
            service_url=service_url,
            reason=CasAuthenticationFailure.INVALID_LOGIN_FORM,
        )
    return candidates[0]


def _follow_service_redirects(
    session: requests.Session,
    redirect_url: str,
    service_url: str,
) -> None:
    current_url = redirect_url
    for redirect_count in range(MAX_SERVICE_REDIRECTS + 1):
        if not _is_exact_https_origin(current_url, service_url):
            raise _unsafe_redirect(service_url)
        response = session.get(
            current_url,
            allow_redirects=False,
            timeout=REQUEST_TIMEOUT,
        )
        try:
            response.raise_for_status()
            _ = response.content
            if response.status_code in (HTTPStatus.FOUND, HTTPStatus.SEE_OTHER):
                location = response.headers.get("Location")
                if location is None or redirect_count == MAX_SERVICE_REDIRECTS:
                    raise _unsafe_redirect(service_url)
                current_url = urljoin(current_url, location)
                continue
            if response.is_redirect or response.is_permanent_redirect:
                raise _unsafe_redirect(service_url)
            return
        finally:
            response.close()
    raise _unsafe_redirect(service_url)


def authenticate_instance(
    credentials: CasCredentials,
    base_url: str,
    session_factory: Callable[[], requests.Session] = requests.Session,
) -> InstanceCookies:
    service_url = f"{base_url}/login/index.php"
    cas_url = f"{CAS_LOGIN_URL}?{urlencode({'service': service_url})}"

    try:
        with session_factory() as session:
            initial_response = session.get(
                cas_url,
                allow_redirects=False,
                timeout=REQUEST_TIMEOUT,
            )
            try:
                initial_response.raise_for_status()
                if (
                    initial_response.is_redirect
                    or initial_response.is_permanent_redirect
                ):
                    raise _unsafe_redirect(service_url)
                soup = BeautifulSoup(initial_response.text, "lxml")
            finally:
                initial_response.close()

            form_data = _login_form_data(soup, service_url)
            form_data.extend(
                (
                    ("username", credentials.username),
                    ("password", credentials.password),
                    ("submit", "LOGIN"),
                ),
            )

            post_response = session.post(
                cas_url,
                data=form_data,
                allow_redirects=False,
                timeout=REQUEST_TIMEOUT,
            )
            post_response.raise_for_status()
            _ = post_response.content
            post_response.close()

            if post_response.status_code in (HTTPStatus.FOUND, HTTPStatus.SEE_OTHER):
                redirect_url = urljoin(
                    cas_url, post_response.headers.get("Location", "")
                )
                _follow_service_redirects(session, redirect_url, service_url)
            elif post_response.is_redirect or post_response.is_permanent_redirect:
                raise _unsafe_redirect(service_url)

            prepared = session.prepare_request(requests.Request("GET", service_url))
            parsed = SimpleCookie()
            parsed.load(prepared.headers.get("Cookie", ""))
    except requests.exceptions.RequestException as error:
        raise CasAuthenticationError(
            service_url=service_url,
            reason=CasAuthenticationFailure.REQUEST,
        ) from error

    missing = tuple(
        name
        for name in ("MoodleSession", "SRVNAME")
        if name not in parsed or not parsed[name].value
    )
    if missing:
        raise CasAuthenticationError(
            service_url=service_url,
            reason=CasAuthenticationFailure.MISSING_COOKIES,
            missing_cookies=missing,
        )
    return InstanceCookies(
        moodle_session=parsed["MoodleSession"].value,
        server_name=parsed["SRVNAME"].value,
    )
