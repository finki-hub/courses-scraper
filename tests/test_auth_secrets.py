from app.auth import CasCredentials, InstanceCookies

CREDENTIAL_VALUE = "credential-value"


def test_cas_credentials_hide_password_from_representations() -> None:
    credentials = CasCredentials("student", CREDENTIAL_VALUE)

    assert CREDENTIAL_VALUE not in repr(credentials)


def test_instance_cookies_hide_session_values_from_representations() -> None:
    cookies = InstanceCookies(CREDENTIAL_VALUE, "routing-value")

    representation = repr(cookies)

    assert CREDENTIAL_VALUE not in representation
    assert "routing-value" not in representation
