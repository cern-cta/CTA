# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import os
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("scheduler", ["pgsched", "objectstore"])
@pytest.mark.parametrize("oracle", [True, False])
def test_stress_deployment_passes_backend_profiles(tmp_path: Path, scheduler: str, oracle: bool) -> None:
    pipeline = (ROOT / ".gitlab/ci/tests-kubernetes.gitlab-ci.yml").read_text()
    # The exercised setup anchor is resolved by YAML; unrelated GitLab references need no expansion.
    _, jobs = yaml.safe_load_all(pipeline.replace("!reference ", ""))
    setup = next(block for block in jobs[".setup-k8s-test-variables"] if "CI_SETUP_CONFIG=" in block)
    stress = jobs["stress-test"]["script"]
    profiles = next(block for block in stress if "STRESS_CTA_VALUES_CHAIN=" in block)
    deploy = next(block for block in stress if "./create_instance.sh" in block)

    # Capture the actual deployment arguments without invoking Kubernetes or Helm.
    stub = tmp_path / "create_instance.sh"
    stub.write_text('#!/bin/bash\nprintf "%s\\0" "$@"\n')
    stub.chmod(0o700)
    env = {
        **os.environ,
        "SCHEDULER_TYPE": scheduler,
        "ENABLE_ORACLE_SUPPORT": str(oracle).lower(),
        "CTA_VALUES_CHAIN": "base-values.yaml",
        "EXTRA_PYTEST_ARGS": "",
        "EXTRA_SPAWN_ARGS": "",
        "NAMESPACE": "test-stress",
        "IMAGE_TAG": "test",
    }
    result = subprocess.run(
        ["/bin/bash", "-eu"],
        input="\n".join((setup, profiles, deploy)).encode(),
        cwd=tmp_path,
        env=env,
        capture_output=True,
        check=True,
    )
    args = result.stdout.decode().rstrip("\0").split("\0")

    expected = (
        "/opt/kubernetes/CTA/scheduler/stresstest-scheduler-postgres-values.yaml"
        if scheduler == "pgsched"
        else "/opt/kubernetes/CTA/scheduler/ci-ceph-values.yaml"
    )
    assert args.count("--scheduler-config") == 1
    assert args[args.index("--scheduler-config") + 1] == expected
    if oracle:
        assert args[args.index("--catalogue-config") + 1] == "/opt/kubernetes/CTA/catalogue/ci-oracle-values.yaml"
    else:
        assert "--catalogue-config" not in args
