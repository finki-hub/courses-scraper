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
    moodle_session: str
    server_name: str | None = None

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


def authenticate_instance(
    credentials: CasCredentials,
    base_url: str,
    session_factory: Callable[[], requests.Session] = requests.Session,
) -> InstanceCookies:
    service_url = f"{base_url}/login/index.php"
    cas_url = f"{CAS_LOGIN_URL}?{urlencode({'service': service_url})}"

    try:
        with session_factory() as session:
            initial_response = session.get(cas_url, timeout=REQUEST_TIMEOUT)
            initial_response.raise_for_status()
            soup = BeautifulSoup(initial_response.text, "lxml")
            initial_response.close()

            form_data: list[tuple[str, str]] = []
            for field in soup.select('input[type="hidden"]'):
                name = field.get("name")
                value = field.get("value", "")
                if isinstance(name, str):
                    form_data.append((name, value if isinstance(value, str) else ""))
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
                redirect_target = urlsplit(redirect_url)
                service_target = urlsplit(service_url)
                if (
                    redirect_target.scheme != "https"
                    or redirect_target.hostname != service_target.hostname
                ):
                    raise CasAuthenticationError(
                        service_url=service_url,
                        reason=CasAuthenticationFailure.UNSAFE_REDIRECT,
                    )
                service_response = session.get(
                    redirect_url,
                    allow_redirects=True,
                    timeout=REQUEST_TIMEOUT,
                )
                service_response.raise_for_status()
                _ = service_response.content
                service_response.close()
            elif post_response.is_redirect or post_response.is_permanent_redirect:
                raise CasAuthenticationError(
                    service_url=service_url,
                    reason=CasAuthenticationFailure.UNSAFE_REDIRECT,
                )

            prepared = session.prepare_request(requests.Request("GET", service_url))
            parsed = SimpleCookie()
            parsed.load(prepared.headers.get("Cookie", ""))
    except requests.exceptions.RequestException as error:
        raise CasAuthenticationError(
            service_url=service_url,
            reason=CasAuthenticationFailure.REQUEST,
        ) from error

    missing = tuple(name for name in ("MoodleSession", "SRVNAME") if name not in parsed)
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
