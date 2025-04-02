import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import patch, Mock
from ctarestapi.main import create_app
from ctarestapi.dependencies import get_catalogue


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("JWKS_ENDPOINT", "https://fake-jwks.local/.well-known/jwks.json")
    mock_catalogue = Mock()

    # We ignore all middleware to ensure we can test the endpoints directly
    # There will be separate tests specifically for the middleware
    with patch.object(FastAPI, "add_middleware", lambda *args, **kwargs: None):
        app = create_app()
        app.dependency_overrides[get_catalogue] = lambda: mock_catalogue

        client = TestClient(app)
        client.mock_catalogue = mock_catalogue  # attach for access in tests
        yield client

        app.dependency_overrides.clear()
