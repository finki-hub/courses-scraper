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

from app.constants import COL_ID, Selectors
from app.profile_parser import parse_profile_html

REQUEST_TIMEOUT: Final = (5, 15)
TRANSPORT_FAILURE_THRESHOLD: Final = 3
USER_AGENT: Final = (
    "courses-scraper/0.1.0 "
    "(+https://github.com/finki-hub/courses-scraper; profile export)"
)
RETRYABLE_STATUSES: Final = frozenset({429, 500, 502, 503, 504})

type Profile = dict[str, str]


@dataclass(frozen=True, slots=True)
class InstanceHttpConfig:
    base_url: str
    cookie: str
    selectors: Selectors
    threads: int


@unique
class EmptyReason(StrEnum):
    HTTP_STATUS = "http_status"
    EMPTY_PROFILE = "empty_profile"
    PARSE_ERROR = "parse_error"


@dataclass(frozen=True, slots=True)
class ProfileSuccess:
    profile: Profile


@dataclass(frozen=True, slots=True)
class ProfileEmpty:
    profile_id: int
    reason: EmptyReason
    status_code: int


@dataclass(frozen=True, slots=True)
class ProfileTransportFailure:
    profile_id: int


type ProfileFetchOutcome = ProfileSuccess | ProfileEmpty | ProfileTransportFailure


@unique
class PreflightFailureReason(StrEnum):
    LOGIN_REDIRECT = "login_redirect"
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
class TransportFailureLimitError(Exception):
    base_url: str
    failure_count: int
    threshold: int

    def __str__(self) -> str:
        return (
            f"aborted {self.base_url} after {self.failure_count} transport failures "
            f"(threshold: {self.threshold})"
        )


@dataclass(frozen=True, slots=True)
class _ExactHostCookieAuth(AuthBase):
    host: str
    cookie: str

    def __call__(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
        request.headers.pop("Cookie", None)
        if request.url is None:
            return request

        target = urlsplit(request.url)
        if target.scheme == "https" and target.hostname == self.host:
            request.headers["Cookie"] = f"MoodleSession={self.cookie}"
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
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=max(10, config.threads),
        pool_maxsize=max(10, config.threads),
    )
    host = urlsplit(config.base_url).hostname
    if host is None:
        raise ValueError(f"invalid instance URL: {config.base_url}")

    session = _ExactHostCookieSession()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers["User-Agent"] = USER_AGENT
    session.auth = _ExactHostCookieAuth(host=host, cookie=config.cookie)
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
        response = session.get(profile_url, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.RequestException:
        return ProfileTransportFailure(profile_id=profile_id)

    if response.status_code != HTTPStatus.OK:
        return ProfileEmpty(
            profile_id=profile_id,
            reason=EmptyReason.HTTP_STATUS,
            status_code=response.status_code,
        )
    try:
        profile = parse_profile_html(response.text, config.selectors)
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
