# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Exercise deployment argument handling without creating Kubernetes resources."""

import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
Invocation = Callable[..., subprocess.CompletedProcess[str]]

# Stop at chart preparation, after connection selection and required Secret checks.
HARNESS = r"""
source "$1"
output="$2"
shift 2
kubectl() {
  printf '%s\n' "$*" >> "$output/checked-secrets"
  [[ "$*" != *'get secret missing '* ]]
}
update_local_cta_chart_dependencies() {
  printf '%s\n' "${infra_config[@]}" > "$output/infra"
  printf '%s\n' "${cta_overrides[@]}" > "$output/overrides"
  printf '%s\n' "${reset_flags[@]}" > "$output/resets"
  printf '%s\n' "${copy_secrets[@]}" > "$output/secrets"
  exit 0
}
create_instance --namespace test --cta-image-tag test "$@"
"""


@pytest.fixture
def invoke(tmp_path: Path) -> Invocation:
    def run(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603
            [
                "/bin/bash",
                "-c",
                HARNESS,
                "test",
                str(ROOT / "ci/orchestration/create_instance.sh"),
                str(tmp_path),
                *args,
            ],
            cwd=ROOT / "ci/orchestration",
            capture_output=True,
            text=True,
            check=False,
        )

    return run


def arguments(tmp_path: Path, name: str) -> list[str]:
    return [line for line in (tmp_path / name).read_text().splitlines() if line]


@pytest.mark.parametrize("catalogue", [False, True])
@pytest.mark.parametrize("scheduler", [False, True])
def test_connection_selection(invoke: Invocation, tmp_path: Path, catalogue: bool, scheduler: bool) -> None:
    flags = []
    for connection, external in (("catalogue", catalogue), ("scheduler", scheduler)):
        if external:
            flags += [f"--{connection}-secret", f"external-{connection}"]
    result = invoke(*flags)
    assert result.returncode == 0, result.stderr
    assert ("presets/infra/catalogue-postgres.yaml" in arguments(tmp_path, "infra")) is not catalogue
    assert ("presets/infra/scheduler-vfs.yaml" in arguments(tmp_path, "infra")) is not scheduler
    for connection, external in (("catalogue", catalogue), ("scheduler", scheduler)):
        assert (f"external-{connection}" in arguments(tmp_path, "secrets")) is external
        assert (
            f"global.{connection}.existingSecret=external-{connection}" in arguments(tmp_path, "overrides")
        ) is external
    assert arguments(tmp_path, "resets") == ["--set", "reset.catalogue=false", "--set", "reset.scheduler=false"]


@pytest.mark.parametrize("connection", ["catalogue", "scheduler"])
@pytest.mark.parametrize("external", [False, True])
def test_explicit_reset_is_independent_of_connection_source(
    invoke: Invocation, tmp_path: Path, connection: str, external: bool
) -> None:
    flags = [f"--reset-{connection}"]
    if external:
        flags += [f"--{connection}-secret", f"ci-{connection}"]
    result = invoke(*flags)
    assert result.returncode == 0, result.stderr
    resets = arguments(tmp_path, "resets")
    assert f"reset.{connection}=true" in resets
    other = "scheduler" if connection == "catalogue" else "catalogue"
    assert f"reset.{other}=false" in resets


def test_ci_can_reset_both_external_connections(invoke: Invocation, tmp_path: Path) -> None:
    result = invoke(
        "--catalogue-secret",
        "ci-catalogue",
        "--scheduler-secret",
        "ci-scheduler",
        "--reset-catalogue",
        "--reset-scheduler",
    )
    assert result.returncode == 0, result.stderr
    assert arguments(tmp_path, "infra") == []
    assert arguments(tmp_path, "resets") == ["--set", "reset.catalogue=true", "--set", "reset.scheduler=true"]


@pytest.mark.parametrize("secret_first", [False, True])
def test_secret_flags_override_values_and_preserve_backend_selection(
    invoke: Invocation, tmp_path: Path, secret_first: bool
) -> None:
    secret = ["--scheduler-secret", "ci-scheduler"]
    values = ["--cta-values", "presets/cta/scheduler/postgres.yaml"]
    result = invoke(*(secret + values if secret_first else values + secret))
    assert result.returncode == 0, result.stderr
    assert arguments(tmp_path, "overrides") == [
        "--values",
        "presets/cta/scheduler/postgres.yaml",
        "--set-string",
        "global.scheduler.existingSecret=ci-scheduler",
    ]


def test_explicit_infrastructure_replaces_defaults(invoke: Invocation, tmp_path: Path) -> None:
    result = invoke(
        "--catalogue-secret",
        "ci-catalogue",
        "--infra-values",
        "presets/infra/external.yaml",
        "--infra-values",
        "presets/infra/scheduler-postgres.yaml",
    )
    assert result.returncode == 0, result.stderr
    assert arguments(tmp_path, "infra") == [
        "--values",
        "presets/infra/external.yaml",
        "--values",
        "presets/infra/scheduler-postgres.yaml",
    ]


def test_repeated_connection_flag_uses_only_final_secret(invoke: Invocation, tmp_path: Path) -> None:
    result = invoke("--catalogue-secret", "missing", "--catalogue-secret", "selected")
    assert result.returncode == 0, result.stderr
    assert arguments(tmp_path, "secrets") == ["selected"]


@pytest.mark.parametrize("option", ["--catalogue-secret", "--scheduler-secret", "--copy-secret"])
@pytest.mark.parametrize("name", ["", "Invalid", "--reset-catalogue", "x" * 254])
def test_invalid_secret_name_is_rejected(invoke: Invocation, tmp_path: Path, option: str, name: str) -> None:
    result = invoke(option, name)
    assert result.returncode != 0
    assert f"{option} requires a Kubernetes Secret name" in result.stdout + result.stderr
    assert not (tmp_path / "infra").exists()


@pytest.mark.parametrize("option", ["--catalogue-secret", "--scheduler-secret", "--copy-secret"])
def test_secret_option_requires_a_value(invoke: Invocation, option: str) -> None:
    result = invoke(option)
    assert result.returncode != 0
    assert f"{option} requires a Kubernetes Secret name" in result.stdout + result.stderr


@pytest.mark.parametrize("option", ["--protected", "--reset-unprotected", "--allow-destructive", "--validate-only"])
def test_removed_options_are_rejected(invoke: Invocation, option: str) -> None:
    result = invoke(option)
    assert result.returncode != 0
    assert f"Unsupported argument: {option}" in result.stdout + result.stderr


@pytest.mark.parametrize("option", ["--catalogue-secret", "--scheduler-secret"])
def test_missing_source_secret_stops_before_chart_preparation(invoke: Invocation, tmp_path: Path, option: str) -> None:
    result = invoke(option, "missing")
    assert result.returncode != 0
    assert "Cannot read requested Secret 'missing'" in result.stdout + result.stderr
    assert not (tmp_path / "infra").exists()


def test_source_namespace_cannot_be_deployment_namespace(invoke: Invocation, tmp_path: Path) -> None:
    result = invoke("--namespace", "default", "--catalogue-secret", "external-catalogue")
    assert result.returncode != 0
    assert "Cannot copy Secrets from default" in result.stdout + result.stderr
    assert not (tmp_path / "infra").exists()
