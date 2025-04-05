import pytest
from ctarestapi.internal.catalogue import CatalogueStatus


def test_health_can_be_done_without_authentication(client_with_auth):
    response = client_with_auth.get("/v0/health")
    assert response.status_code == 200


def test_status_ok(client):
    client.mock_catalogue.get_status.return_value = CatalogueStatus(
        status="ok", schemaVersion="1.2.3", backend="postgresql", host="cta-db", database="cta"
    )

    response = client.get("/v0/status")
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "ok"
    assert data["components"]["catalogue"]["status"] == "ok"
    assert data["components"]["catalogue"]["schemaVersion"] == "1.2.3"

    client.mock_catalogue.get_status.assert_called_once()


def test_status_degraded(client):
    client.mock_catalogue.get_status.return_value = CatalogueStatus(
        status="unavailable", schemaVersion="unknown", backend="postgresql", host="cta-db", database="cta"
    )

    response = client.get("/v0/status")
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "unavailable"
    assert data["components"]["catalogue"]["status"] == "unavailable"
    assert data["components"]["catalogue"]["schemaVersion"] == "unknown"

    client.mock_catalogue.get_status.assert_called_once()
