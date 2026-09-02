from dataclasses import dataclass
from enum import StrEnum, unique
from typing import Final

TRANSPORT_FAILURE_THRESHOLD: Final = 3
TRANSPORT_FAILURE_RATE_THRESHOLD: Final = 0.5

type Profile = dict[str, str]


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


@unique
class ProfileFailureReason(StrEnum):
    REDIRECT = "redirect"
    HTTP_STATUS = "http_status"
    LOGIN_RESPONSE = "login_response"


@dataclass(frozen=True, slots=True)
class ProfileRequestError(Exception):
    profile_id: int
    reason: ProfileFailureReason
    status_code: int

    def __str__(self) -> str:
        return (
            f"profile {self.profile_id} request failed: {self.reason.value} "
            f"({self.status_code})"
        )


type ProfileFetchOutcome = ProfileSuccess | ProfileEmpty | ProfileTransportFailure


@dataclass(frozen=True, slots=True)
class TransportFailureLimitError(Exception):
    base_url: str
    failure_count: int
    outcome_count: int
    minimum_outcomes: int = TRANSPORT_FAILURE_THRESHOLD
    rate_threshold: float = TRANSPORT_FAILURE_RATE_THRESHOLD

    def __str__(self) -> str:
        return (
            f"aborted {self.base_url} after {self.failure_count} of "
            f"{self.outcome_count} observed outcomes were transport failures "
            f"(limit: >{self.rate_threshold:.0%} after {self.minimum_outcomes} "
            "outcomes or batch completion)"
        )


def transport_failure_rate_exceeded(
    failure_count: int,
    outcome_count: int,
    request_count: int,
) -> bool:
    enough_evidence = (
        outcome_count >= TRANSPORT_FAILURE_THRESHOLD or outcome_count == request_count
    )
    return (
        failure_count > 0
        and enough_evidence
        and failure_count / outcome_count > TRANSPORT_FAILURE_RATE_THRESHOLD
    )
