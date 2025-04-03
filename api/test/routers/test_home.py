import pytest


def test_status_can_be_done_without_authentication(client_with_auth):
    response = client_with_auth.get("/status")
    assert response.status_code == 200
