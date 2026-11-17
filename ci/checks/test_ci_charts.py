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

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

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
        str(PRESETS / "cta/base.yaml"),
        "-f",
        str(PRESETS / "cta/auth/frontend-admin-jwt.yaml"),
        "-f",
        str(PRESETS / "cta/auth/frontend-wfe-jwt.yaml"),
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
        str(PRESETS / "infra/catalogue-postgres.yaml"),
        "-f",
        str(PRESETS / "infra/scheduler-postgres.yaml"),
        "-f",
        str(PRESETS / "infra/dcache.yaml"),
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


@pytest.mark.parametrize("client_name", ["cta-cli", "custom-client"])
def test_api_client_has_no_database_access(charts: Path, client_name: str) -> None:
    objects = render(
        charts,
        "cta-admin-tools",
        "--set",
        f"client.enabled=true,client.fullnameOverride={client_name}",
        "-f",
        str(PRESETS / "cta/base.yaml"),
        "-f",
        str(PRESETS / "cta/auth/frontend-admin-jwt.yaml"),
    )
    workload = next(obj for obj in objects if obj["kind"] == "StatefulSet")
    assert workload["metadata"]["name"] == client_name
    labels = workload["spec"]["template"]["metadata"]["labels"]
    assert workload["metadata"]["labels"]["app.kubernetes.io/name"] == client_name
    assert workload["spec"]["selector"]["matchLabels"].items() <= labels.items()
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
        # Consume the drop confirmation so the producer cannot race with an early mock exit.
        confirmation = "cat >/dev/null\n" if action == "drop" else ""
        executable.write_text(f'#!/bin/bash\n{confirmation}printf "%s\n" "{action}" >> "$COMMAND_LOG"\n')
        executable.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    monkeypatch.setenv("COMMAND_LOG", str(command_log))
    monkeypatch.setenv("SCHEMA_VERSION", "latest")
    monkeypatch.setenv("SCHEDULER_BACKEND", "postgres")
    run_tool("bash", str(CHARTS / "cta-admin-tools/scripts" / f"reset_{component}.sh"))
    assert command_log.read_text().splitlines() == ["drop", "create"]


@pytest.mark.parametrize("preset", ["postgres", "vfs", "ceph"])
def test_infrastructure_presets(charts: Path, preset: str) -> None:
    filename = f"infra/scheduler-{preset}.yaml" if preset != "ceph" else "infra/external.yaml"
    args = ["-f", str(PRESETS / filename)]
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
        str(PRESETS / "cta/scheduler/ceph.yaml"),
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


@pytest.mark.parametrize("backend", ["vfs", "ceph", "postgres"])
@pytest.mark.parametrize(("catalogue", "scheduler"), [(False, False), (True, False), (False, True), (True, True)])
def test_optional_reset_jobs(charts: Path, backend: str, catalogue: bool, scheduler: bool) -> None:
    objects = render(
        charts,
        "cta-admin-tools",
        "--set",
        f"global.scheduler.backend={backend},reset.catalogue={str(catalogue).lower()},reset.scheduler={str(scheduler).lower()}",
        "--set-string",
        "reset.catalogueSchemaVersion=15.0",
    )
    jobs = {obj["metadata"]["name"]: obj for obj in objects if obj["kind"] == "Job"}
    assert set(jobs) == {
        f"test-reset-{name}" for name, enabled in [("catalogue", catalogue), ("scheduler", scheduler)] if enabled
    }
    for name, job in jobs.items():
        database = name.rsplit("-", 1)[1]
        assert "helm.sh/hook" not in job["metadata"].get("annotations", {})
        assert job["spec"]["activeDeadlineSeconds"] == 240
        assert job["spec"]["backoffLimit"] == 0
        assert "ttlSecondsAfterFinished" not in job["spec"]
        spec = job["spec"]["template"]["spec"]
        assert spec["restartPolicy"] == "Never"
        container = spec["containers"][0]
        assert container["command"] == ["bash", f"/scripts/reset_{database}.sh"]
        env = {entry["name"]: entry["value"] for entry in container["env"]}
        assert env == ({"SCHEMA_VERSION": "15.0"} if database == "catalogue" else {"SCHEDULER_BACKEND": backend})
        volumes = {volume["name"]: volume for volume in spec["volumes"]}
        assert f"{database}-config" in volumes
        assert ("vfs-storage-volume" in volumes) == (database == "scheduler" and backend == "vfs")
        config = next(obj for obj in objects if obj["metadata"]["name"] == volumes["scripts"]["configMap"]["name"])
        assert f"reset_{database}.sh" in config["data"]
        if database == "catalogue" or backend == "postgres":
            init = spec["initContainers"][0]
            assert init["command"] == ["bash", "/scripts/wait_database.sh", f"/etc/cta/cta-{database}.conf"]
            assert init["image"] == "docker.io/postgres:17"
            assert init["volumeMounts"][1]["name"] == f"{database}-config"
            assert "wait_database.sh" in config["data"]
        else:
            assert "initContainers" not in spec


def test_database_readiness_retries_only_read_only_query(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    connection = "postgresql://test:password@database/test"
    config = tmp_path / "database.conf"
    config.write_text(f"postgresql:{connection}")
    calls = tmp_path / "calls"
    psql = tmp_path / "psql"
    psql.write_text('#!/bin/bash\nprintf "%s\\n" "$*" >> "$CALLS"\n[[ $(wc -l < "$CALLS") -gt 1 ]]\n')
    psql.chmod(0o755)
    sleep = tmp_path / "sleep"
    sleep.write_text("#!/bin/bash\nexit 0\n")
    sleep.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    monkeypatch.setenv("CALLS", str(calls))
    result = run_tool("bash", str(CHARTS / "cta-admin-tools/scripts/wait_database.sh"), str(config))
    assert calls.read_text().splitlines() == [f"{connection} --no-psqlrc --set=ON_ERROR_STOP=1 --command=SELECT 1"] * 2
    assert "Waiting for database connection" in result.stdout
    assert "Database connection ready" in result.stdout
    assert "password" not in result.stdout + result.stderr


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
        ("cta/scheduler/postgres.yaml", "schedulermaintenancecleanup"),
        ("cta/scheduler/postgres-external.yaml", "schedulermaintenancecleanup"),
        ("cta/scheduler/vfs.yaml", "garbagecollect"),
        ("cta/scheduler/ceph.yaml", "garbagecollect"),
    ],
)
def test_scheduler_presets_include_maintenance(charts: Path, preset: str, routine: str) -> None:
    objects = render(
        charts,
        "cta",
        "-f",
        str(PRESETS / "cta/scheduler/postgres.yaml"),
        "-f",
        str(PRESETS / preset),
        "--set",
        "global.image.tag=test",
    )
    names = {obj["metadata"]["name"] for obj in objects if obj["kind"] == "Deployment"}
    assert f"test-rt-{routine}" in names


def test_upgrade_shares_connection_overrides(tmp_path: Path) -> None:
    source = (REPO / "ci/orchestration/upgrade_cta_instance.sh").read_text()
    function = source[source.index("upgrade_instance() {") : source.index('\nupgrade_instance "$@"')]
    values = tmp_path / "scheduler override.yaml"
    values.write_text("global:\n  scheduler:\n    existingSecret: replacement\n")
    script = (
        "set -e\nupdate_local_cta_chart_dependencies() { :; }\ncheck_helm_installed() { :; }\n"
        'kubectl() { :; }\nlog_run() { printf "%s\\n" "$@"; }\n'
        + function
        + '\nupgrade_instance --namespace test --cta-values "$1" --cta-image-tag updated '
        + "--cta-image-registry registry.example\n"
    )
    result = run_tool("bash", "-c", script, "test", str(values))
    lines = result.stdout.splitlines()
    assert lines.count(str(values)) == 2
    assert lines.count("global.image.tag=updated") == 2
    assert lines.count("global.image.registry=registry.example") == 2
    assert not any("global.image.repository=" in line for line in lines)
    assert "--reuse-values" in lines
    assert lines[-4:] == ["--set", "reset.catalogue=false", "--set", "reset.scheduler=false"]


@pytest.mark.parametrize(("prometheus", "otel"), [(False, False), (True, False), (False, True), (True, True)])
def test_optional_telemetry_dependencies(charts: Path, prometheus: bool, otel: bool) -> None:
    objects = render(
        charts,
        "ci-infra",
        "-f",
        str(PRESETS / "infra/telemetry.yaml"),
        "--set",
        f"prometheus.enabled={str(prometheus).lower()},otel.enabled={str(otel).lower()}",
    )
    assert not any(
        obj["kind"] in {"ClusterRole", "ClusterRoleBinding", "CustomResourceDefinition", "PersistentVolume"}
        for obj in objects
    )
    assert all(obj["metadata"].get("namespace") in {None, "", "test"} for obj in objects)
    for obj in objects:
        if obj["kind"] == "RoleBinding":
            assert obj["roleRef"]["kind"] == "Role"
    services = {obj["metadata"]["name"]: obj for obj in objects if obj["kind"] == "Service"}
    assert ("prometheus-server" in services) == prometheus
    assert ("otel-opentelemetry-collector" in services) == otel
    if prometheus:
        assert "prometheus-kube-state-metrics" in services
        assert "prometheus-prometheus-node-exporter" in services
        for kind in ["Role", "RoleBinding"]:
            names = {obj["metadata"]["name"] for obj in objects if obj["kind"] == kind}
            assert "prometheus-kube-state-metrics" in names
        config = next(
            obj for obj in objects if obj["kind"] == "ConfigMap" and obj["metadata"]["name"] == "prometheus-server"
        )
        scrape_config = yaml.safe_load(config["data"]["prometheus.yml"])["scrape_configs"]
        assert {job["job_name"] for job in scrape_config} == {
            "prometheus",
            "otel-collector",
            "kube-state-metrics",
            "node-exporter",
        }
        assert all("kubernetes_sd_configs" not in job for job in scrape_config)
        targets = {target for job in scrape_config for group in job["static_configs"] for target in group["targets"]}
        assert targets == {
            "localhost:9090",
            "otel-opentelemetry-collector:8889",
            "prometheus-kube-state-metrics:8080",
            "prometheus-prometheus-node-exporter:9100",
        }
        workloads = {obj["metadata"]["name"]: obj for obj in objects if obj["kind"] == "Deployment"}
        assert workloads["prometheus-server"]["spec"]["template"]["spec"]["automountServiceAccountToken"] is False
        containers = workloads["prometheus-kube-state-metrics"]["spec"]["template"]["spec"]["containers"]
        args = containers[0]["args"]
        assert "--namespaces=test" in args
        resources = next(arg.removeprefix("--resources=") for arg in args if arg.startswith("--resources="))
        assert set(resources.split(",")) == {
            "pods",
            "deployments",
            "statefulsets",
            "daemonsets",
            "replicasets",
            "jobs",
            "cronjobs",
            "persistentvolumeclaims",
        }
    if otel:
        ports = {port["port"] for port in services["otel-opentelemetry-collector"]["spec"]["ports"]}
        assert {4318, 8889} <= ports
    assert not any(obj["kind"] == "PersistentVolumeClaim" for obj in objects)


@pytest.mark.parametrize("telemetry", ["none", "local", "central"])
def test_creation_keeps_provisioning_separate_and_preserves_override_arguments(tmp_path: Path, telemetry: str) -> None:
    source = (REPO / "ci/orchestration/create_instance.sh").read_text()
    source = source[: source.rindex('\ncreate_instance "$@"')]
    # Stub external actions, but execute the actual argument parser and deployment sequence.
    source = "\n".join(line for line in source.splitlines() if not line.startswith('source "'))
    base = tmp_path / "base values.yaml"
    infra = tmp_path / "private infra.yaml"
    override = tmp_path / "final override.yaml"
    for path in [base, infra, override]:
        path.write_text("{}\n")
    script = (
        source
        + """
check_deploy_commands() { :; }
update_local_cta_chart_dependencies() { :; }
generate_tape_values_files() { taped_config=tapes.yaml; rmcd_config=robot.yaml; }
log_task() { :; }
log_warn() { :; }
log_success() { :; }
die() { echo "$*" >&2; exit 1; }
die_usage() { die "$@"; }
helm() { [[ $1 == list || $1 == dependency ]]; }
kubectl() { [[ " $* " != *" get secret "* ]]; }
log_run() { printf 'COMMAND\\n'; printf '%s\\n' "$@"; printf 'END\\n'; }
"""
        + f'cd "{REPO}/ci/orchestration"\n'
        + "create_instance -n test -i test --eos-enabled false --reset-catalogue --reset-scheduler "
        + '--cta-values "$1" --infra-values "$2" --cta-values "$3" --cta-values "$1" --telemetry "$4"\n'
    )
    result = run_tool("bash", "-s", "--", str(base), str(infra), str(override), telemetry, input_text=script)
    commands = [block.split("\nEND")[0].splitlines() for block in result.stdout.split("COMMAND\n")[1:]]
    infra_install, tools_install, infra_wait, cta_install = commands
    assert infra_install[:4] == ["helm", "upgrade", "--install", "ci-infra"]
    assert str(infra) in infra_install
    assert str(base) not in infra_install
    assert ("presets/infra/telemetry.yaml" in infra_install) == (telemetry == "local")
    assert infra_wait[:3] == ["helm", "upgrade", "ci-infra"]
    assert "--wait-for-jobs" in infra_wait
    assert "--wait-for-jobs" in tools_install
    assert "reset.catalogue=true" in tools_install
    assert "reset.scheduler=true" in tools_install
    assert "reset.catalogueSchemaVersion=latest" in tools_install
    for command in [tools_install, cta_install]:
        assert str(infra) not in command
        value_files = [command[i + 1] for i, arg in enumerate(command) if arg == "--values"]
        assert value_files == [
            "presets/cta/base.yaml",
            "presets/cta/auth/frontend-wfe-jwt.yaml",
            "presets/cta/auth/frontend-admin-jwt.yaml",
            "presets/cta/catalogue/postgres.yaml",
            "presets/cta/scheduler/vfs.yaml",
            *([f"presets/cta/telemetry/{telemetry}.yaml"] if telemetry != "none" else []),
            str(base),
            str(override),
            str(base),
        ]


@pytest.mark.parametrize(
    "preset",
    [
        "cta/catalogue/postgres",
        "cta/catalogue/oracle",
        "cta/scheduler/postgres",
        "cta/scheduler/vfs",
        "cta/scheduler/ceph",
        "cta/scheduler/postgres-external",
    ],
)
def test_consumer_presets_contain_only_public_settings(preset: str) -> None:
    values = yaml.safe_load((PRESETS / f"{preset}.yaml").read_text())
    assert set(values) <= {"global", "maintd"}
    assert set(values["global"]) <= {"catalogue", "scheduler"}
    for connection in values["global"].values():
        assert set(connection) <= {"backend", "existingSecret", "existingClaim"}


@pytest.mark.parametrize("backend", ["postgres", "ceph", "vfs"])
def test_scheduler_overlays_replace_backend_cleanup(charts: Path, backend: str) -> None:
    objects = render(
        charts,
        "cta",
        "-f",
        str(PRESETS / "cta/scheduler/vfs.yaml"),
        "-f",
        str(PRESETS / "cta/scheduler/postgres.yaml"),
        "-f",
        str(PRESETS / f"cta/scheduler/{backend}.yaml"),
        "--set",
        "global.image.tag=test",
    )
    routines = {
        obj["metadata"]["name"] for obj in objects if obj["kind"] == "Deployment" and obj["spec"]["replicas"] > 0
    }
    assert ("test-rt-garbagecollect" in routines) == (backend != "postgres")
    assert ("test-rt-queuecleanup" in routines) == (backend != "postgres")
    assert ("test-rt-schedulermaintenancecleanup" in routines) == (backend == "postgres")
    # Check all rendered configs, including zero-replica workloads left by composed overlays.
    common = {"disk_report_archive", "disk_report_retrieve", "repack_expand", "repack_report"}
    cleanup = (
        {
            "user_active_queue_cleanup",
            "repack_active_queue_cleanup",
            "user_pending_queue_cleanup",
            "repack_pending_queue_cleanup",
            "scheduler_maintenance_cleanup",
        }
        if backend == "postgres"
        else {"garbage_collect", "queue_cleanup"}
    )
    expected = common | cleanup
    configs = {
        obj["metadata"]["name"]: tomllib.loads(obj["data"]["cta-maintd.toml"])["routines"]
        for obj in objects
        if obj["kind"] == "ConfigMap" and "cta-maintd.toml" in obj.get("data", {})
    }
    assert len(configs) == len(expected)
    for config in configs.values():
        assert set(config) == expected | {"cycle_sleep_interval_secs", "max_cycle_duration_secs"}
        assert sum(config[name]["enabled"] for name in expected) == 1
    deployments = [obj for obj in objects if obj["kind"] == "Deployment" and "-rt-" in obj["metadata"]["name"]]
    assert len(deployments) == len(expected)
    for deployment in deployments:
        spec = deployment["spec"]["template"]["spec"]
        volume = next(v for v in spec["volumes"] if v["name"] == "cta-maintd-config")
        assert volume["configMap"]["name"] in configs
        # The runtime reads telemetry configuration even when export is disabled.
        mount = next(m for m in spec["containers"][0]["volumeMounts"] if m["mountPath"] == "/etc/cta/cta-otel.yaml")
        volume = next(v for v in spec["volumes"] if v["name"] == mount["name"])
        config = next(obj for obj in objects if obj["metadata"]["name"] == volume["configMap"]["name"])
        assert mount["subPath"] in config["data"]


def test_eos_values_preserve_order_and_spaces(tmp_path: Path) -> None:
    source = (REPO / "ci/orchestration/deploy_eos.sh").read_text()
    function = source[source.index("deploy() {") : source.index('\ndeploy "$@"')]
    values = tmp_path / "EOS override.yaml"
    values.write_text("{}\n")
    script = (
        'set -e\ncheck_helm_installed() { :; }\nkubectl() { :; }\nhelm() { echo "dependency ok"; }\n'
        'log_run() { printf "%s\\n" "$@"; }\n'
        + function
        + '\ndeploy --namespace test --eos-values "$1" --eos-values "$1"\n'
    )
    result = run_tool("bash", "-s", "--", str(values), input_text=script)
    args = result.stdout.splitlines()
    value_files = [args[i + 1] for i, arg in enumerate(args) if arg == "--values"]
    assert value_files == ["presets/eos/auth-jwt.yaml", str(values), str(values)]


def test_dcache_values_and_explicit_image_overrides(tmp_path: Path) -> None:
    source = (REPO / "ci/orchestration/deploy_dcache.sh").read_text()
    function = source[source.index("deploy() {") : source.index('\ndeploy "$@"')]
    values = tmp_path / "dCache override.yaml"
    values.write_text("dcache:\n  database:\n    existingSecret: external-database\n")
    script = (
        'set -e\ncheck_helm_installed() { :; }\nkubectl() { :; }\nhelm() { echo "dependency ok"; }\n'
        'log_run() { printf "%s\\n" "$@"; }\n'
        + function
        + '\ndeploy --namespace test --dcache-values "$1" --dcache-values "$1" '
        + "--dcache-image-repository registry.example/team/dcache --dcache-image-tag custom\n"
    )
    result = run_tool("bash", "-s", "--", str(values), input_text=script)
    args = result.stdout.splitlines()
    value_files = [args[i + 1] for i, arg in enumerate(args) if arg == "--values"]
    assert value_files == [str(values), str(values)]
    overrides = [args[i + 1] for i, arg in enumerate(args) if arg == "--set-string"]
    assert overrides == [
        "dcache.image.registry=registry.example",
        "dcache.image.repository=team/dcache",
        "dcache.image.tag=custom",
    ]
    assert args.index("--values") < args.index("--set-string")
