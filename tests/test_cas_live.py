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


def test_live_cas_authenticates_both_instances() -> None:
    # Given dedicated CAS credentials supplied only to the live CI job.
    username = os.environ.get("CAS_USERNAME")
    password = os.environ.get("CAS_PASSWORD")
    if not username or not password:
        pytest.skip("live CAS credentials are not configured")
        return
    credentials = CasCredentials(username=username, password=password)

    # When each Courses service authenticates and accesses its profile page.
    try:
        for base_url, selectors in (
            (base_urls["new"], selectors_new),
            (base_urls["old"], selectors_old),
        ):
            cookies = authenticate_instance(credentials, base_url)
            config = InstanceHttpConfig(
                base_url=base_url,
                cookies=cookies,
                selectors=selectors,
                threads=1,
            )
            with create_session(config) as session:
                preflight_instance(session, config)
    except (CasAuthenticationError, PreflightError) as error:
        pytest.fail(str(error), pytrace=False)

    # Then both services returned authenticated, parseable profile pages.
