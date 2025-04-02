import pytest

def test_get_drives(client):
    client.mock_catalogue.drives.get_all_drives.return_value = []

    response = client.get("/drives/")
    assert response.status_code == 200
    client.mock_catalogue.drives.get_all_drives.assert_called_once_with(limit=100, offset=0)


def test_get_drive_found(client):
    client.mock_catalogue.drives.get_drive.return_value = {"drive_name": "test"}

    response = client.get("/drives/test")
    assert response.status_code == 200
    client.mock_catalogue.drives.get_drive.assert_called_once_with(drive_name="test")


def test_get_drive_not_found(client):
    client.mock_catalogue.drives.get_drive.return_value = None

    response = client.get("/drives/test")
    assert response.status_code == 404
    assert response.json()["detail"] == "Drive not found"


def test_update_drive_state_up(client):
    client.mock_catalogue.drives.set_drive_up.return_value = True

    response = client.put("/drives/test/state", json={"desired_state": "up", "reason": "fix"})
    assert response.status_code == 200
    client.mock_catalogue.drives.set_drive_up.assert_called_once_with("test", "fix")


def test_update_drive_state_down_with_reason(client):
    client.mock_catalogue.drives.set_drive_down.return_value = True

    response = client.put("/drives/test/state?force=true", json={"desired_state": "down", "reason": "maintenance"})
    assert response.status_code == 200
    client.mock_catalogue.drives.set_drive_down.assert_called_once_with("test", "maintenance", True)


def test_update_drive_state_down_missing_reason(client):
    response = client.put("/drives/test/state", json={"desired_state": "down"})
    assert response.status_code == 422


def test_update_drive_comment(client):
    client.mock_catalogue.drives.update_drive_comment.return_value = True

    response = client.put("/drives/test/comment", json={"comment": "new comment"})
    assert response.status_code == 200
    client.mock_catalogue.drives.update_drive_comment.assert_called_once_with("test", "new comment")


def test_update_drive_comment_too_short(client):
    response = client.put("/drives/test/comment", json={"comment": ""})
    assert response.status_code == 422


def test_delete_drive(client):
    response = client.delete("/drives/test")
    assert response.status_code == 501
    assert response.json()["detail"] == "Not implemented yet."
