import os

import pytest

from app.auth import CasAuthenticationError, CasCredentials, authenticate_instance
from app.constants import base_urls, selectors_new, selectors_old
from app.http import (
    InstanceHttpConfig,
    PreflightError,
    create_session,
    preflight_instance,
)


@pytest.mark.parametrize("instance", ["new", "old"])
def test_live_cas_authenticates_instance(instance: str) -> None:
    _authenticate_and_preflight_instance(instance)


def _authenticate_and_preflight_instance(instance: str) -> None:
    # Given dedicated CAS credentials supplied only to the live CI job.
    username = os.environ.get("CAS_USERNAME")
    password = os.environ.get("CAS_PASSWORD")
    if not username or not password:
        pytest.skip("live CAS credentials are not configured")
        return
    credentials = CasCredentials(username=username, password=password)

    # When the selected Courses service authenticates and accesses its profile page.
    base_url = base_urls[instance]
    selectors = {"new": selectors_new, "old": selectors_old}[instance]
    try:
        cookies = authenticate_instance(credentials, base_url)
        config = InstanceHttpConfig(
            base_url=base_url,
            cookies=cookies,
            selectors=selectors,
            threads=1,
        )
        with create_session(config) as session:
            preflight_instance(session, config)
    except CasAuthenticationError as error:
        # Transport causes can contain service tickets; expose only safe fields.
        raise CasAuthenticationError(
            error.service_url, error.reason, error.missing_cookies
        ) from None
    except PreflightError as error:
        raise PreflightError(error.base_url, error.reason, error.status_code) from None

    # Then the selected service returned an authenticated, parseable profile page.
