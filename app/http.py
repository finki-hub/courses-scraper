from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum, unique
from http import HTTPStatus
from typing import Final
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from urllib3.util.retry import Retry

from app.auth import InstanceCookies
from app.constants import COL_ID, Selectors
from app.profile_outcomes import (
    TRANSPORT_FAILURE_RATE_THRESHOLD,
    TRANSPORT_FAILURE_THRESHOLD,
    EmptyReason,
    Profile,
    ProfileEmpty,
    ProfileFailureReason,
    ProfileFetchOutcome,
    ProfileRequestError,
    ProfileSuccess,
    ProfileTransportFailure,
    TransportFailureLimitError,
    transport_failure_rate_exceeded,
)
from app.profile_parser import LoginPageError, parse_profile_html

__all__ = (
    "REQUEST_TIMEOUT",
    "RETRYABLE_STATUSES",
    "TRANSPORT_FAILURE_RATE_THRESHOLD",
    "TRANSPORT_FAILURE_THRESHOLD",
    "USER_AGENT",
    "EmptyReason",
    "InstanceHttpConfig",
    "PreflightError",
    "PreflightFailureReason",
    "Profile",
    "ProfileEmpty",
    "ProfileFailureReason",
    "ProfileFetchOutcome",
    "ProfileRequestError",
    "ProfileSuccess",
    "ProfileTransportFailure",
    "TransportFailureLimitError",
    "create_session",
    "fetch_profile",
    "preflight_instance",
    "transport_failure_rate_exceeded",
)

REQUEST_TIMEOUT: Final = (5, 15)
USER_AGENT: Final = (
    "courses-scraper/0.1.0 "
    "(+https://github.com/finki-hub/courses-scraper; profile export)"
)
RETRYABLE_STATUSES: Final = frozenset({429, 500, 502, 503, 504})


@dataclass(frozen=True, slots=True)
class InstanceHttpConfig:
    base_url: str
    cookies: InstanceCookies
    selectors: Selectors
    threads: int


@unique
class PreflightFailureReason(StrEnum):
    LOGIN_REDIRECT = "login_redirect"
    LOGIN_RESPONSE = "login_response"
    REDIRECT = "redirect"
    HTTP_STATUS = "http_status"
    EMPTY_PROFILE = "empty_profile"
    PARSE_ERROR = "parse_error"
    TRANSPORT = "transport"


@dataclass(frozen=True, slots=True)
class PreflightError(Exception):
    base_url: str
    reason: PreflightFailureReason
    status_code: int | None = None

    def __str__(self) -> str:
        status = "" if self.status_code is None else f" ({self.status_code})"
        return f"preflight failed for {self.base_url}: {self.reason.value}{status}"


@dataclass(frozen=True, slots=True)
class _ExactHostCookieAuth(AuthBase):
    host: str
    cookies: InstanceCookies

    def __call__(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
        request.headers.pop("Cookie", None)
        if request.url is None:
            return request

        target = urlsplit(request.url)
        if target.scheme == "https" and target.hostname == self.host:
            request.headers["Cookie"] = self.cookies.as_header()
        return request


class _ExactHostCookieSession(requests.Session):
    auth: _ExactHostCookieAuth

    def rebuild_auth(
        self,
        prepared_request: requests.PreparedRequest,
        response: requests.Response,
    ) -> None:
        self.auth(prepared_request)


def create_session(config: InstanceHttpConfig) -> requests.Session:
    retry_strategy = Retry(
        total=5,
        status_forcelist=RETRYABLE_STATUSES,
        backoff_factor=1,
        allowed_methods=frozenset({"HEAD", "GET", "OPTIONS"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=max(10, config.threads),
        pool_maxsize=max(10, config.threads),
    )
    instance_url = urlsplit(config.base_url)
    host = instance_url.hostname
    if instance_url.scheme != "https" or host is None:
        raise ValueError(f"instance URL must use HTTPS: {config.base_url}")

    session = _ExactHostCookieSession()
    session.mount("https://", adapter)
    session.headers["User-Agent"] = USER_AGENT
    session.auth = _ExactHostCookieAuth(host=host, cookies=config.cookies)
    return session


def preflight_instance(
    session: requests.Session,
    config: InstanceHttpConfig,
) -> None:
    try:
        response = session.get(
            f"{config.base_url}/user/profile.php",
            allow_redirects=False,
            timeout=REQUEST_TIMEOUT,
        )
    except requests.exceptions.RequestException as error:
        raise PreflightError(
            base_url=config.base_url,
            reason=PreflightFailureReason.TRANSPORT,
        ) from error

    if response.is_redirect or response.is_permanent_redirect:
        location = response.headers.get("Location", "")
        reason = (
            PreflightFailureReason.LOGIN_REDIRECT
            if "/login/" in location
            else PreflightFailureReason.REDIRECT
        )
        raise PreflightError(
            base_url=config.base_url,
            reason=reason,
            status_code=response.status_code,
        )
    if response.status_code != HTTPStatus.OK:
        raise PreflightError(
            base_url=config.base_url,
            reason=PreflightFailureReason.HTTP_STATUS,
            status_code=response.status_code,
        )

    try:
        profile = parse_profile_html(response.text, config.selectors)
    except LoginPageError as error:
        raise PreflightError(
            base_url=config.base_url,
            reason=PreflightFailureReason.LOGIN_RESPONSE,
            status_code=response.status_code,
        ) from error
    except (AttributeError, KeyError, TypeError) as error:
        raise PreflightError(
            base_url=config.base_url,
            reason=PreflightFailureReason.PARSE_ERROR,
            status_code=response.status_code,
        ) from error
    if not profile:
        raise PreflightError(
            base_url=config.base_url,
            reason=PreflightFailureReason.EMPTY_PROFILE,
            status_code=response.status_code,
        )


def fetch_profile(
    session: requests.Session,
    profile_id: int,
    config: InstanceHttpConfig,
) -> ProfileFetchOutcome:
    profile_url = f"{config.base_url}/user/profile.php?id={profile_id}&showallcourses=1"
    try:
        response = session.get(
            profile_url,
            allow_redirects=False,
            timeout=REQUEST_TIMEOUT,
        )
    except requests.exceptions.RequestException:
        return ProfileTransportFailure(profile_id=profile_id)

    if response.is_redirect or response.is_permanent_redirect:
        raise ProfileRequestError(
            profile_id=profile_id,
            reason=ProfileFailureReason.REDIRECT,
            status_code=response.status_code,
        )
    if response.status_code == HTTPStatus.NOT_FOUND:
        return ProfileEmpty(
            profile_id=profile_id,
            reason=EmptyReason.HTTP_STATUS,
            status_code=response.status_code,
        )
    if response.status_code != HTTPStatus.OK:
        raise ProfileRequestError(
            profile_id=profile_id,
            reason=ProfileFailureReason.HTTP_STATUS,
            status_code=response.status_code,
        )
    if "/login/" in urlsplit(response.url).path:
        raise ProfileRequestError(
            profile_id=profile_id,
            reason=ProfileFailureReason.LOGIN_RESPONSE,
            status_code=response.status_code,
        )
    try:
        profile = parse_profile_html(response.text, config.selectors)
    except LoginPageError as error:
        raise ProfileRequestError(
            profile_id=profile_id,
            reason=ProfileFailureReason.LOGIN_RESPONSE,
            status_code=response.status_code,
        ) from error
    except (AttributeError, KeyError, TypeError):
        return ProfileEmpty(
            profile_id=profile_id,
            reason=EmptyReason.PARSE_ERROR,
            status_code=response.status_code,
        )
    if not profile:
        return ProfileEmpty(
            profile_id=profile_id,
            reason=EmptyReason.EMPTY_PROFILE,
            status_code=response.status_code,
        )

    profile[COL_ID] = str(profile_id)
    return ProfileSuccess(profile=profile)
