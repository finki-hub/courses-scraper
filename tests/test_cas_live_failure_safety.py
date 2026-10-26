"""Offline coverage of the live test's credential-safe failure boundary."""

import traceback
from io import BytesIO
from unittest.mock import MagicMock

import pytest
import requests

from app.auth import CasAuthenticationError, InstanceCookies, authenticate_instance
from app.constants import base_urls
from app.http import PreflightError, preflight_instance
from tests import test_cas_live


@pytest.mark.parametrize("instance", ["new", "old"])
@pytest.mark.parametrize("stage", ["authentication", "preflight"])
def test_live_failure_traceback_hides_transport_secrets(
    monkeypatch: pytest.MonkeyPatch, instance: str, stage: str
) -> None:
    monkeypatch.setenv("CAS_USERNAME", "offline-user")
    monkeypatch.setenv("CAS_PASSWORD", "offline-password")
    ticket = "ST-FAKE-REVIEW-TICKET"
    sensitive_detail = "credential-bearing-cause"
    response = requests.Response()
    response.status_code = 503
    response.url = f"{base_urls[instance]}/login/index.php?ticket={ticket}"
    response.reason = sensitive_detail
    response._content = b""
    response.raw = BytesIO()
    session = MagicMock(spec=requests.Session)
    session.__enter__.return_value = session
    session.get.return_value = response
    authenticate = MagicMock(return_value=InstanceCookies("offline-session"))
    preflight = MagicMock()
    monkeypatch.setattr(test_cas_live, "authenticate_instance", authenticate)
    monkeypatch.setattr(test_cas_live, "preflight_instance", preflight)
    monkeypatch.setattr(
        test_cas_live, "create_session", MagicMock(return_value=session)
    )

    expected_error: type[CasAuthenticationError] | type[PreflightError]
    if stage == "authentication":
        # Exercise the actual HTTP-status wrapping, not a fabricated safe error.
        authenticate.side_effect = lambda credentials, base_url: (
            authenticate_instance(
                credentials, base_url, session_factory=lambda: session
            )
            if base_url == base_urls[instance]
            else InstanceCookies("offline-session")
        )
        expected_error = CasAuthenticationError
        safe_reason = "request_failed"
    else:
        transport_error = requests.exceptions.ConnectionError(
            f"{response.url} {sensitive_detail}"
        )
        session.get.side_effect = transport_error
        preflight.side_effect = lambda active_session, config: (
            preflight_instance(active_session, config)
            if config.base_url == base_urls[instance]
            else None
        )
        expected_error = PreflightError
        safe_reason = "transport"

    with pytest.raises(expected_error) as caught:
        test_cas_live.test_live_cas_authenticates_both_instances()

    rendered = "".join(traceback.format_exception(caught.value))
    assert ticket not in rendered
    assert sensitive_detail not in rendered
    assert safe_reason in rendered
    assert base_urls[instance] in rendered
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__
    expected_calls = 1 if instance == "new" else 2
    assert authenticate.call_count == expected_calls
    assert preflight.call_count == expected_calls - (stage == "authentication")
