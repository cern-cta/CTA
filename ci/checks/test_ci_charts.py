# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Render the real CI charts without contacting a Kubernetes cluster."""

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Optional

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
CHARTS = REPO / "ci/orchestration/helm"
PRESETS = REPO / "ci/orchestration/presets"


def run_tool(executable: str, *args: str, input_text: Optional[str] = None) -> subprocess.CompletedProcess[str]:
    command = shutil.which(executable)
    assert command is not None
    # Call trusted local tools with repository-owned inputs; never invoke a shell interpreter on arguments.
    return subprocess.run(  # noqa: S603
        [command, *args], input=input_text, check=True, capture_output=True, text=True
    )


@pytest.fixture(scope="module")
def charts(tmp_path_factory: pytest.TempPathFactory) -> Path:
    if not shutil.which("helm"):
        pytest.skip("Helm is required for chart rendering")
    destination = tmp_path_factory.mktemp("charts") / "helm"
    shutil.copytree(CHARTS, destination, ignore=shutil.ignore_patterns("charts", "Chart.lock"))
    for name in [
        "common",
        "frontend-admin",
        "frontend-wfe",
        "taped",
        "rmcd",
        "maintd",
        "cta-admin-tools",
        "cta",
    ]:
        run_tool(
            "helm",
            "dependency",
            "update",
            str(destination / name),
            "--skip-refresh",
            "--repository-config",
            "/dev/null",
        )
    # Stage the real pinned remote charts from cache; unit tests never fetch dependencies.
    infra = destination / "ci-infra"
    dependencies = infra / "charts"
    dependencies.mkdir(exist_ok=True)
    cache = Path(os.environ.get("HELM_REPOSITORY_CACHE", Path.home() / ".cache/helm/repository"))
    for dependency in yaml.safe_load((infra / "Chart.yaml").read_text())["dependencies"]:
        archive = cache / f"{dependency['name']}-{dependency['version']}.tgz"
        if archive.exists():
            shutil.copy(archive, dependencies / archive.name)
    return destination


def render(charts: Path, chart: str, *args: str) -> list[dict[str, Any]]:
    if chart == "ci-infra":
        for dependency in yaml.safe_load((charts / chart / "Chart.yaml").read_text())["dependencies"]:
            archive = f"{dependency['name']}-{dependency['version']}.tgz"
            if not (charts / chart / "charts" / archive).exists():
                pytest.skip(f"Cache {archive} with helm dependency update in a temporary ci-infra copy")
    result = run_tool("helm", "template", "test", str(charts / chart), "--namespace", "test", *args)
    return [item for item in yaml.safe_load_all(result.stdout) if item]


def pod_specs(objects: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        obj["spec"] if obj["kind"] == "Pod" else obj["spec"]["template"]["spec"]
        for obj in objects
        if obj["kind"] in {"Pod", "Job", "Deployment", "StatefulSet"}
    ]


@pytest.mark.parametrize("backend", ["postgres", "vfs", "ceph"])
def test_services_consume_only_references(charts: Path, backend: str) -> None:
    objects = render(
        charts,
        "cta",
        "-f",
        str(PRESETS / "dev-cta-common.yaml"),
        "-f",
        str(PRESETS / "frontend-admin/auth-jwt.yaml"),
        "-f",
        str(PRESETS / "frontend-wfe/auth-jwt.yaml"),
        "--set",
        "global.image.tag=test",
        "--set",
        f"global.scheduler.backend={backend}",
        "--set",
        "global.catalogue.existingSecret=external-catalogue",
        "--set",
        "global.scheduler.existingSecret=external-scheduler",
        "--set",
        "global.scheduler.existingClaim=external-vfs",
        "--set",
        "taped.drives[0].name=test-drive",
        "--set",
        "taped.drives[0].device=/dev/nst0",
        "--set",
        "taped.drives[0].controlPath=/dev/sg0",
        "--set",
        "taped.drives[0].logicalLibraryName=test-library",
    )
    assert not any(obj["kind"] in {"Secret", "Job", "PersistentVolumeClaim"} for obj in objects)
    assert not any("cli" in obj["metadata"]["name"] for obj in objects)
    references = []
    for spec in pod_specs(objects):
        volumes = {volume["name"]: volume for volume in spec.get("volumes", [])}
        if "catalogue-config" not in volumes:
            continue
        assert volumes["catalogue-config"]["secret"]["secretName"] == "external-catalogue"
        assert volumes["scheduler-config"]["secret"]["secretName"] == "external-scheduler"
        references.append(spec["containers"][0]["name"])
        if backend == "vfs":
            assert volumes["vfs-storage-volume"]["persistentVolumeClaim"]["claimName"] == "external-vfs"
        if backend == "ceph":
            assert "cta-ceph-volume" not in volumes
            assert any(
                mount["name"] == "scheduler-config" and mount["mountPath"] == "/etc/ceph"
                for mount in spec["containers"][0]["volumeMounts"]
            )
        for container in spec["containers"]:
            names = {env["name"] for env in container.get("env", [])}
            assert not names & {"MY_NAME", "MY_NAMESPACE", "INSTANCE_NAME", "SCHEDULER_BACKEND"}
            assert not any(name.startswith("SCHEDULER_CEPH_") for name in names)
    assert len(references) >= 4


def test_infra_provisions_three_databases_without_resets(charts: Path) -> None:
    objects = render(
        charts,
        "ci-infra",
        "-f",
        str(PRESETS / "dev-catalogue-postgres-values.yaml"),
        "-f",
        str(PRESETS / "dev-scheduler-postgres-values.yaml"),
        "-f",
        str(PRESETS / "dev-dcache-infra-values.yaml"),
    )
    databases = [obj for obj in objects if obj["kind"] == "StatefulSet"]
    assert len(databases) == 3
    for spec in pod_specs(databases):
        assert spec["containers"][0]["image"] == "docker.io/postgres:17"
        env = {entry["name"]: entry for entry in spec["containers"][0]["env"]}
        assert "PGHOST" not in env
        for key in ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"]:
            assert env[key]["valueFrom"]["secretKeyRef"]["key"] == key
    assert [obj["metadata"]["name"] for obj in objects if obj["kind"] == "Job"] == ["test-gen-secrets-job"]
    connections = {obj["metadata"]["name"]: obj["stringData"] for obj in objects if obj["kind"] == "Secret"}
    assert "cta-catalogue.conf" in connections["cta-catalogue-conf"]
    assert "cta-scheduler.conf" in connections["cta-scheduler-conf"]
    assert "dcache.conf" in connections["dcache-database"]


def test_external_connections_and_oracle(charts: Path, tmp_path: Path) -> None:
    # An external connection needs no PostgreSQL instance or backend-specific chart logic.
    assert not any(obj["kind"] in {"StatefulSet", "Secret"} for obj in render(charts, "ci-infra"))
    values = tmp_path / "external.yaml"
    values.write_text(
        yaml.safe_dump({"secretFiles": {"external-catalogue": {"cta-catalogue.conf": "oracle:user/test@database"}}})
    )
    objects = render(charts, "ci-infra", "-f", str(values))
    secret = next(obj for obj in objects if obj["metadata"]["name"] == "external-catalogue")
    assert secret["stringData"]["cta-catalogue.conf"] == "oracle:user/test@database"
    assert not any(obj["kind"] == "StatefulSet" for obj in objects)


def test_arbitrary_database_names(charts: Path, tmp_path: Path) -> None:
    values = tmp_path / "databases.yaml"
    values.write_text(yaml.safe_dump({"postgres": {"instances": {"alpha": {}, "beta": {}, "gamma": {}}}}))
    objects = render(charts, "ci-infra", "-f", str(values))
    instances = [obj for obj in objects if obj["kind"] == "StatefulSet"]
    assert {obj["metadata"]["name"] for obj in instances} == {"alpha", "beta", "gamma"}
    for name in ["alpha", "beta", "gamma"]:
        secret = next(obj for obj in objects if obj["metadata"]["name"] == f"{name}-credentials")
        assert secret["stringData"]["PGHOST"] == name
        assert secret["stringData"]["POSTGRES_DB"] == name
        assert secret["stringData"]["POSTGRES_USER"] == name
    assert all(spec["containers"][0]["name"] == "postgres" for spec in pod_specs(instances))


def test_database_connection_file_uses_instance_credentials(charts: Path, tmp_path: Path) -> None:
    values = tmp_path / "database.yaml"
    values.write_text(
        yaml.safe_dump(
            {
                "postgres": {
                    "instances": {
                        "example": {
                            "database": "test db",
                            "username": "test user",
                            "password": "test/password",
                            "secretName": "example-connection",
                            "extraSecretData": {"connection.txt": "{{ .connection.uri }}"},
                        }
                    }
                }
            }
        )
    )
    objects = render(charts, "ci-infra", "-f", str(values))
    secret = next(obj for obj in objects if obj["metadata"]["name"] == "example-connection")
    assert secret["stringData"]["connection.txt"] == "postgresql://test%20user:test%2Fpassword@example/test%20db"
    configured = yaml.safe_load(values.read_text())["postgres"]["instances"]["example"]
    assert secret["stringData"]["POSTGRES_PASSWORD"] == configured["password"]


def test_admin_tools_start_without_schema_operations(charts: Path) -> None:
    assert not pod_specs(render(charts, "cta-admin-tools"))
    objects = render(charts, "cta-admin-tools", "--set", "database.enabled=true,global.scheduler.backend=postgres")
    assert not any(obj["kind"] == "Job" for obj in objects)
    spec = pod_specs(objects)[0]
    assert not spec.get("initContainers")
    assert {container["name"] for container in spec["containers"]} == {"admin"}
    for container in spec["containers"]:
        assert "sleep infinity" in container["command"][-1]
        assert not container.get("readinessProbe")
        assert not any(env["name"] == "OPERATION" for env in container.get("env", []))


def test_liquibase_pod(charts: Path) -> None:
    objects = render(
        charts,
        "cta-admin-tools",
        "--set",
        "liquibase.enabled=true",
        "--set",
        "liquibase.catalogueSourceVersion=15.0",
        "--set",
        "liquibase.catalogueDestinationVersion=16.0",
    )
    workload = next(obj for obj in objects if obj["kind"] != "ConfigMap")
    assert workload["kind"] == "Pod"
    spec = pod_specs([workload])[0]
    assert spec["volumes"][-1]["secret"]["secretName"] == "cta-catalogue-conf"
    run_tool("bash", "-n", input_text=spec["containers"][0]["args"][0])


def test_api_client_has_no_database_access(charts: Path) -> None:
    objects = render(
        charts,
        "cta-admin-tools",
        "--set",
        "client.enabled=true",
        "-f",
        str(PRESETS / "dev-cta-common.yaml"),
        "-f",
        str(PRESETS / "frontend-admin/auth-jwt.yaml"),
    )
    workload = next(obj for obj in objects if obj["kind"] == "StatefulSet")
    assert workload["metadata"]["name"] == "cta-cli"
    assert not any(
        volume["name"] in {"catalogue-config", "scheduler-config"}
        for volume in workload["spec"]["template"]["spec"]["volumes"]
    )


def test_dcache_uses_infra_secret(charts: Path) -> None:
    # Use a real cached dependency, never helper stubs or a network fetch during unit tests.
    cache = Path(os.environ.get("HELM_REPOSITORY_CACHE", Path.home() / ".cache/helm/repository"))
    archive = cache / "dcache-0.1.0.tgz"
    if not archive.exists():
        pytest.skip("Cache the pinned dCache dependency with helm dependency update before running this test")
    dependencies = charts / "dcache/charts"
    dependencies.mkdir(exist_ok=True)
    shutil.copy(archive, dependencies / archive.name)
    objects = render(charts, "dcache", "--set", "dcache.database.existingSecret=external-dcache")
    for spec in pod_specs(objects):
        assert not any(container["image"].startswith("postgres:") for container in spec["containers"])
        if spec["containers"][0]["name"] == "client":
            continue
        volumes = {volume["name"]: volume for volume in spec.get("volumes", [])}
        if "database-credentials" in volumes:
            assert volumes["database-credentials"]["secret"]["secretName"] == "external-dcache"
            assert "emptyDir" in volumes["dcache-config"]
            script = spec["initContainers"][0]["args"][0]
            assert "/database/POSTGRES_PASSWORD" in script
            run_tool("sh", "-n", input_text=script)


@pytest.mark.parametrize("component", ["catalogue", "scheduler"])
def test_reset_scripts_drop_and_create(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, component: str) -> None:
    command_log = tmp_path / "commands"
    command_log.touch()
    # Replace the native executables; no database or cluster is contacted.
    for action in ["create", "drop"]:
        executable = tmp_path / f"cta-{component}-schema-{action}"
        executable.write_text(f'#!/bin/bash\nprintf "%s\n" "{action}" >> "$COMMAND_LOG"\n')
        executable.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    monkeypatch.setenv("COMMAND_LOG", str(command_log))
    monkeypatch.setenv("SCHEMA_VERSION", "latest")
    monkeypatch.setenv("SCHEDULER_BACKEND", "postgres")
    run_tool("bash", str(CHARTS / "cta-admin-tools/scripts" / f"reset_{component}.sh"))
    assert command_log.read_text().splitlines() == ["drop", "create"]


@pytest.mark.parametrize("preset", ["postgres", "vfs", "ceph"])
def test_infrastructure_presets(charts: Path, preset: str) -> None:
    args = ["-f", str(PRESETS / f"dev-scheduler-{preset}-values.yaml")]
    objects = render(charts, "ci-infra", *args)
    secrets = {obj["metadata"]["name"]: obj["stringData"] for obj in objects if obj["kind"] == "Secret"}
    if preset == "ceph":
        assert "cta-scheduler-conf" not in secrets
        assert "ceph-conf" not in secrets
        return
    connection = secrets["cta-scheduler-conf"]["cta-scheduler.conf"]
    if preset == "postgres":
        assert connection.startswith("postgresql:postgresql://")
    elif preset == "vfs":
        assert connection == "/objectstore"
        assert any(obj["kind"] == "PersistentVolumeClaim" for obj in objects)


def test_generic_storage_claims(charts: Path, tmp_path: Path) -> None:
    values = tmp_path / "storage.yaml"
    values.write_text(
        yaml.safe_dump(
            {
                "persistentVolumeClaims": {
                    "scratch": {"size": "1Gi"},
                    "shared": {
                        "size": "2Gi",
                        "storageClass": "-",
                        "accessModes": ["ReadWriteMany"],
                        "annotations": {"example.org/purpose": "test"},
                    },
                }
            }
        )
    )
    claims = {
        obj["metadata"]["name"]: obj
        for obj in render(charts, "ci-infra", "-f", str(values))
        if obj["kind"] == "PersistentVolumeClaim"
    }
    assert set(claims) == {"scratch", "shared"}
    assert "storageClassName" not in claims["scratch"]["spec"]
    assert claims["scratch"]["spec"]["accessModes"] == ["ReadWriteOnce"]
    assert claims["shared"]["spec"]["storageClassName"] == ""
    assert claims["shared"]["spec"]["accessModes"] == ["ReadWriteMany"]
    assert claims["shared"]["metadata"]["annotations"] == {"example.org/purpose": "test"}


def test_extra_connection_files_cannot_override_database_credentials(charts: Path, tmp_path: Path) -> None:
    values = tmp_path / "invalid.yaml"
    values.write_text(
        yaml.safe_dump(
            {"postgres": {"instances": {"example": {"extraSecretData": {"POSTGRES_USER": "different-user"}}}}}
        )
    )
    with pytest.raises(subprocess.CalledProcessError) as error:
        render(charts, "ci-infra", "-f", str(values))
    assert "extraSecretData cannot override POSTGRES_USER" in error.value.stderr


def test_ceph_admin_uses_bundled_scheduler_secret(charts: Path) -> None:
    objects = render(
        charts,
        "cta-admin-tools",
        "-f",
        str(PRESETS / "dev-scheduler-ceph-values.yaml"),
        "--set",
        "database.enabled=true",
    )
    spec = pod_specs(objects)[0]
    volumes = {volume["name"]: volume for volume in spec["volumes"]}
    assert volumes["scheduler-config"]["secret"]["secretName"] == "cta-ceph-scheduler-conf"
    mounts = spec["containers"][0]["volumeMounts"]
    assert {mount["mountPath"] for mount in mounts if mount["name"] == "scheduler-config"} == {
        "/etc/cta/cta-scheduler.conf",
        "/etc/ceph",
    }
    assert not any("hostPath" in volume for volume in volumes.values())


def test_legacy_vfs_is_rejected(charts: Path) -> None:
    with pytest.raises(subprocess.CalledProcessError) as error:
        render(charts, "cta-admin-tools", "--set", "database.enabled=true,global.scheduler.backend=vfsDeprecated")
    assert "global.scheduler.backend must be postgres, ceph or vfs" in error.value.stderr


@pytest.mark.parametrize("failure", ["", "catalogue", "scheduler"])
def test_preparation_waits_for_commands_and_propagates_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    source = (REPO / "ci/orchestration/create_instance.sh").read_text()
    function = source[source.index("prepare_cta_databases() {") : source.index("create_instance() {")]
    command_log = tmp_path / "commands"
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        '#!/bin/bash\nprintf "%s\n" "$*" >> "$COMMAND_LOG"\n'
        'if [[ -n "$FAILURE" && "$*" == *reset_"$FAILURE".sh ]]; then exit 42; fi\n'
    )
    kubectl.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    monkeypatch.setenv("COMMAND_LOG", str(command_log))
    monkeypatch.setenv("FAILURE", failure)
    script = (
        'set -e\nlog_run() { "$@"; }\n'
        + function
        + 'prepare_cta_databases test true true latest\necho services >> "$COMMAND_LOG"\n'
    )
    if failure:
        with pytest.raises(subprocess.CalledProcessError):
            run_tool("bash", input_text=script)
    else:
        run_tool("bash", input_text=script)
    commands = command_log.read_text().splitlines()
    assert "/scripts/reset_catalogue.sh" in commands[0]
    if failure == "catalogue":
        assert len(commands) == 1
    else:
        assert "-c admin" in commands[1]
        assert "/scripts/reset_scheduler.sh" in commands[1]
        if failure == "scheduler":
            assert len(commands) == 2
        else:
            assert commands[2] == "services"
    # The actual deployment calls preparation before installing services.
    assert source.index('  prepare_cta_databases "$namespace"') < source.index(
        "  log_run helm upgrade --install cta helm/cta"
    )


@pytest.mark.parametrize(("catalogue", "scheduler"), [("false", "false"), ("true", "false"), ("false", "true")])
def test_reset_flags_skip_unrequested_commands(catalogue: str, scheduler: str) -> None:
    source = (REPO / "ci/orchestration/create_instance.sh").read_text()
    function = source[source.index("prepare_cta_databases() {") : source.index("create_instance() {")]
    result = run_tool(
        "bash",
        input_text='set -e\nlog_run() { printf "%s\n" "$*"; }\n'
        + function
        + f"prepare_cta_databases test {catalogue} {scheduler} latest\n",
    )
    assert ("reset_catalogue.sh" in result.stdout) == (catalogue == "true")
    assert ("reset_scheduler.sh" in result.stdout) == (scheduler == "true")
    assert "RESET=" not in result.stdout


@pytest.mark.parametrize("global_override", [False, True])
def test_tools_share_image_configuration(charts: Path, global_override: bool) -> None:
    args = [
        "--set",
        "database.enabled=true,client.enabled=true",
        "--set",
        "image.registry=local.example,image.repository=tools,image.tag=local",
        "--set",
        "image.pullPolicy=Always,image.pullSecrets[0]=local-pull",
    ]
    expected_image = "local.example/tools:local"
    expected_policy = "Always"
    expected_pull_name = "local-pull"
    if global_override:
        args += [
            "--set",
            "global.image.registry=global.example,global.image.tag=shared",
            "--set",
            "global.image.pullPolicy=Never,global.image.pullSecrets[0]=global-pull",
        ]
        expected_image = "global.example/tools:shared"
        expected_policy = "Never"
        expected_pull_name = "global-pull"
    objects = render(charts, "cta-admin-tools", *args)
    for spec in pod_specs(objects):
        assert spec["imagePullSecrets"] == [{"name": expected_pull_name}]
        for container in spec["containers"] + spec.get("initContainers", []):
            assert container["image"] == expected_image
            assert container["imagePullPolicy"] == expected_policy


@pytest.mark.parametrize(
    ("preset", "routine"),
    [
        ("dev-scheduler-postgres-values.yaml", "schedulermaintenancecleanup"),
        ("stress-scheduler-postgres-values.yaml", "schedulermaintenancecleanup"),
        ("dev-scheduler-vfs-values.yaml", "garbagecollect"),
        ("dev-scheduler-ceph-values.yaml", "garbagecollect"),
    ],
)
def test_scheduler_presets_include_maintenance(charts: Path, preset: str, routine: str) -> None:
    objects = render(charts, "cta", "-f", str(PRESETS / preset), "--set", "global.image.tag=test")
    names = {obj["metadata"]["name"] for obj in objects if obj["kind"] == "Deployment"}
    assert f"test-rt-{routine}" in names


def test_upgrade_shares_connection_overrides(tmp_path: Path) -> None:
    source = (REPO / "ci/orchestration/upgrade_cta_instance.sh").read_text()
    function = source[source.index("upgrade_instance() {") : source.index("\ncheck_helm_installed\n")]
    values = tmp_path / "scheduler override.yaml"
    values.write_text("global:\n  scheduler:\n    existingSecret: replacement\n")
    script = (
        "set -e\nupdate_local_cta_chart_dependencies() { :; }\n"
        'kubectl() { :; }\nlog_run() { printf "%s\\n" "$@"; }\n'
        + function
        + '\nupgrade_instance --namespace test --scheduler-config "$1" --cta-image-tag updated\n'
    )
    result = run_tool("bash", "-c", script, "test", str(values))
    lines = result.stdout.splitlines()
    assert lines.count(str(values)) == 2
    assert lines.count("global.image.tag=updated") == 2
    assert "--reuse-values" in lines


@pytest.mark.parametrize(("prometheus", "otel"), [(False, False), (True, False), (False, True), (True, True)])
def test_optional_telemetry_dependencies(charts: Path, prometheus: bool, otel: bool) -> None:
    objects = render(
        charts,
        "ci-infra",
        "-f",
        str(PRESETS / "dev-telemetry-infra-values.yaml"),
        "--set",
        f"prometheus.enabled={str(prometheus).lower()},otel.enabled={str(otel).lower()}",
    )
    services = {obj["metadata"]["name"]: obj for obj in objects if obj["kind"] == "Service"}
    assert ("prometheus-server" in services) == prometheus
    assert ("otel-opentelemetry-collector" in services) == otel
    if prometheus:
        assert "prometheus-kube-state-metrics" in services
        assert "prometheus-prometheus-node-exporter" in services
        for kind in ["ClusterRole", "ClusterRoleBinding"]:
            names = {obj["metadata"]["name"] for obj in objects if obj["kind"] == kind}
            assert {"prometheus-server", "prometheus-kube-state-metrics"} <= names
        config = next(
            obj for obj in objects if obj["kind"] == "ConfigMap" and obj["metadata"]["name"] == "prometheus-server"
        )
        assert "otel-opentelemetry-collector:8889" in config["data"]["prometheus.yml"]
    if otel:
        ports = {port["port"] for port in services["otel-opentelemetry-collector"]["spec"]["ports"]}
        assert {4318, 8889} <= ports
    assert not any(obj["kind"] == "PersistentVolumeClaim" for obj in objects)
