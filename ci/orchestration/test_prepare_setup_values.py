# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
from pathlib import Path
from typing import Any

import pytest
from prepare_setup_values import apply_profile, main, merge_values, read_values, runtime_values

ROOT = Path(__file__).parent


@pytest.fixture
def setup_values() -> dict[str, Any]:
    return read_values(ROOT / "helm/ci-setup/values.yaml")


@pytest.mark.parametrize("backend", ["vfs", "postgres"])
def test_presets_share_connection_references(setup_values: dict[str, Any], backend: str) -> None:
    values = merge_values(setup_values, read_values(ROOT / f"presets/dev-ci-setup-{backend}-scheduler-values.yaml"))
    runtime = runtime_values(values)["global"]

    assert runtime["catalogue"] == values["catalogueReset"]["connection"]
    assert runtime["scheduler"] == values["schedulerReset"]["connection"]
    assert runtime["objectstore"]["enabled"] == (backend == "vfs")


def test_external_profiles_replace_local_databases(setup_values: dict[str, Any]) -> None:
    apply_profile(
        setup_values,
        "catalogue",
        {
            "backend": "oracle",
            "oracleConfig": {"username": "test-user", "password": "test-only", "database": "test-db"},
        },
    )
    apply_profile(
        setup_values,
        "scheduler",
        {
            "backend": "ceph",
            "cephConfig": {"id": "test-id", "pool": "test-pool", "namespace": "test-ns", "key": "test-key"},
        },
    )

    assert setup_values["postgres"]["databases"] == []
    secret = setup_values["extraObjects"][0]
    assert secret["stringData"]["cta-catalogue.conf"] == "oracle:test-user/test-only@test-db"
    assert setup_values["objectstore"]["ceph"]["key"] == "test-key"
    runtime = runtime_values(setup_values)["global"]
    assert runtime["objectstore"]["backend"] == "ceph"
    assert runtime["objectstore"]["ceph"] == {"id": "test-id", "pool": "test-pool", "namespace": "test-ns"}


@pytest.mark.parametrize("remote", [True, False])
def test_postgres_profile_replaces_only_selected_database(setup_values: dict[str, Any], remote: bool) -> None:
    apply_profile(
        setup_values,
        "scheduler",
        {
            "backend": "postgres",
            "postgresConfig": {
                "remote": remote,
                "server": "test-postgres",
                "database": "test-db",
                "username": "test-user",
                "password": "test-only",
            },
        },
    )

    databases = setup_values["postgres"]["databases"]
    assert databases[0]["secretName"] == "cta-catalogue-conf"
    if remote:
        assert len(databases) == 1
        assert setup_values["extraObjects"][0]["stringData"]["cta-scheduler.conf"] == (
            "postgresql:postgresql://test-user:test-only@test-postgres/test-db"
        )
    else:
        assert len(databases) == 2
        assert databases[1]["serviceName"] == "test-postgres"
        assert databases[1]["database"] == "test-db"


def test_cli_writes_private_matching_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "prepare_setup_values.py",
            "--ci-setup-config",
            str(ROOT / "presets/dev-ci-setup-vfs-scheduler-values.yaml"),
            "--output-dir",
            str(tmp_path),
        ],
    )
    main()

    setup = json.loads((tmp_path / "setup-values.json").read_text())
    runtime = json.loads((tmp_path / "cta-values.json").read_text())
    assert runtime == runtime_values(setup)
    assert (tmp_path / "setup-values.json").stat().st_mode & 0o777 == 0o600


def test_unknown_backend_is_rejected(setup_values: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="Unsupported scheduler backend"):
        apply_profile(setup_values, "scheduler", {"backend": "unsupported"})


def test_vfs_profile_preserves_custom_storage(setup_values: dict[str, Any]) -> None:
    apply_profile(
        setup_values,
        "scheduler",
        {
            "backend": "vfs",
            "vfsConfig": {
                "storageClass": "custom-nfs",
                "size": "100Gi",
                "accessModes": ["ReadWriteMany"],
                "annotations": {"example.org/storage": "shared"},
            },
        },
    )

    assert setup_values["objectstore"]["vfs"] == {
        "storageClassName": "custom-nfs",
        "size": "100Gi",
        "accessModes": ["ReadWriteMany"],
        "annotations": {"example.org/storage": "shared"},
    }


def test_vfs_profile_disables_dynamic_provisioning(setup_values: dict[str, Any]) -> None:
    apply_profile(setup_values, "scheduler", {"backend": "vfs", "vfsConfig": {"storageClass": "-"}})

    assert setup_values["objectstore"]["vfs"]["storageClassName"] == ""
    assert setup_values["objectstore"]["vfs"]["size"] == "10Gi"


def test_minimal_vfs_profile_keeps_preset_storage(setup_values: dict[str, Any]) -> None:
    setup_values["objectstore"]["vfs"]["storageClassName"] = "preset-storage"
    apply_profile(setup_values, "scheduler", {"backend": "vfs"})

    assert setup_values["objectstore"]["vfs"]["storageClassName"] == "preset-storage"
    assert setup_values["objectstore"]["vfs"]["accessModes"] == ["ReadWriteOnce"]
