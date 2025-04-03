import pytest
import datetime


def test_valid_jwt_returns_200(client_with_auth):
    exp = datetime.datetime.now() + datetime.timedelta(minutes=5)
    valid_token = client_with_auth.keyPair.generate_valid_jwt(exp=exp)

    response = client_with_auth.get("/protected", headers={"Authorization": f"Bearer {valid_token}"})
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_expired_token_returns_401(client_with_auth):
    exp = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)
    expired_token = client_with_auth.keyPair.generate_valid_jwt(exp=exp)

    response = client_with_auth.get("/protected", headers={"Authorization": f"Bearer {expired_token}"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or expired token"


def test_no_expiration_present_returns_401(client_with_auth):
    kp = client_with_auth.keyPair
    invalid_token = kp.generate_jwt(
        headers={"kid": kp.get_kid()}, body={}, pem=kp.get_pem(), algorithm=kp.get_algorithm()
    )

    response = client_with_auth.get("/protected", headers={"Authorization": f"Bearer {invalid_token}"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or expired token"


def test_algo_none_returns_401(client_with_auth):
    exp = datetime.datetime.now() + datetime.timedelta(minutes=5)
    kp = client_with_auth.keyPair
    invalid_token = kp.generate_jwt(headers={"kid": kp.get_kid()}, body={"exp": exp}, pem="", algorithm=None)

    response = client_with_auth.get("/protected", headers={"Authorization": f"Bearer {invalid_token}"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or expired token"


def test_incorrect_signature_returns_401(client_with_auth):
    exp = datetime.datetime.now() + datetime.timedelta(minutes=5)
    kp = client_with_auth.keyPair
    token = kp.generate_jwt(
        headers={"kid": kp.get_kid()}, body={"exp": exp}, pem=kp.get_pem(), algorithm=kp.get_algorithm()
    )
    parts = token.split(".")
    assert len(parts) == 3

    # Tamper the signature (part 2)
    parts[2] = "invalidstuff"
    invalid_token = ".".join(parts)

    print(token)
    print(invalid_token)

    response = client_with_auth.get("/protected", headers={"Authorization": f"Bearer {invalid_token}"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or expired token"


def test_nonexisting_kid_returns_401(client_with_auth):
    exp = datetime.datetime.now() + datetime.timedelta(minutes=5)
    kp = client_with_auth.keyPair
    invalid_token = kp.generate_jwt(
        headers={"kid": "idontexist"}, body={"exp": exp}, pem=kp.get_pem(), algorithm=kp.get_algorithm()
    )

    response = client_with_auth.get("/protected", headers={"Authorization": f"Bearer {invalid_token}"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or expired token"


def test_missing_kid_returns_401(client_with_auth):
    exp = datetime.datetime.now() + datetime.timedelta(minutes=5)
    kp = client_with_auth.keyPair
    invalid_token = kp.generate_jwt(headers={}, body={"exp": exp}, pem=kp.get_pem(), algorithm=kp.get_algorithm())

    response = client_with_auth.get("/protected", headers={"Authorization": f"Bearer {invalid_token}"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or expired token"


def test_missing_token_returns_401(client_with_auth):
    response = client_with_auth.get("/protected")
    assert response.status_code == 401
    assert response.json()["detail"] == "Missing or invalid Authorization header"
