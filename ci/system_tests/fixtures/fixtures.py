# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import shutil
import sys
import uuid
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pytest
from _pytest.fixtures import SubRequest

from ..helpers.hosts import (
    CtaAdminApiHost,
    CtaCliHost,
    CtaMaintdHost,
    CtaRmcdHost,
    CtaTapedHost,
    CtaWorkflowApiHost,
    DiskClientHost,
    DiskInstanceHost,
    EosClientHost,
    EosMgmHost,
)
from ..helpers.test_config import TestConfig
from ..helpers.test_env import TestEnv

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

# This file could be split into multiple files eventually when necessary

_TESTS_DIRECTORY = Path(__file__).resolve().parent.parent / "tests"
_LIFECYCLE_STYLES = {
    "setup": ("SETUP", "yellow"),
    "verification": ("VERIFICATION", "purple"),
    "teardown": ("TEARDOWN", "blue"),
}

#####################################################################################################################
# General
#####################################################################################################################


def get_test_heading(test_path: Path, test_name: str) -> tuple[str, str]:
    """Return the displayed test name and color for a test path."""
    try:
        relative_path = test_path.resolve().relative_to(_TESTS_DIRECTORY)
    except ValueError:
        return test_name, "cyan"

    phase = relative_path.parts[0]
    phase_style = _LIFECYCLE_STYLES.get(phase)
    if phase_style is None:
        return test_name, "cyan"

    phase_name, color = phase_style
    return f"[{phase_name}] {test_name}", color


@pytest.fixture(autouse=True)
def make_tests_look_pretty(request: SubRequest) -> Iterator[None]:
    """The only purpose of this fixture is to make the test output easier to read
    in particular by more clearly visually separating different test cases
    """
    terminal_writer = request.config.get_terminal_writer()
    terminal_width = shutil.get_terminal_size().columns

    # construct the magic separators
    test_name, color = get_test_heading(request.node.path, request.node.name)
    test_title = f" {test_name} "  # leave 2 spaces around the name
    side = (terminal_width - len(test_title)) // 2
    line = "=" * side + test_title + "=" * (terminal_width - side - len(test_title))
    separator: str = "—" * terminal_width
    color_option = {color: True}

    terminal_writer.write("\n" + line + "\n\n", bold=True, **color_option)
    yield
    terminal_writer.write(f"\n\n{separator}", **color_option)


@pytest.fixture(scope="session")
def error_whitelist() -> set[str]:
    """Mutable whitelist that individual test cases can add errors to"""
    return set()  # mutable whitelist shared between all tests


@pytest.fixture(scope="session")
def project_json() -> dict[str, Any]:
    project_json_path = (Path(__file__).resolve().parent / ".." / ".." / "project.json").resolve()
    return json.loads(project_json_path.read_text(encoding="utf-8"))


#####################################################################################################################
# Test-specific
#####################################################################################################################


@pytest.fixture(scope="session")
def test_config(request: SubRequest) -> TestConfig:
    config_path = request.config.getoption("--test-config")
    if not isinstance(config_path, Path):
        raise pytest.UsageError("--test-config must be a filesystem path")
    try:
        with config_path.open("rb") as config_file:
            return tomllib.load(config_file)
    except FileNotFoundError as error:
        raise pytest.UsageError(f"--test-config file not found: {config_path}") from error


@pytest.fixture(scope="session")
def krb5_realm(test_config: TestConfig) -> str:
    """Kerberos realm used in the tests"""
    return test_config["tests"]["krb5_realm"]


@pytest.fixture(scope="session")
def postgres_scheduler_enabled(cta_cli: CtaCliHost) -> bool:
    # Not very robust; something to improve in the future. Maybe with a label on the entire cluster
    return json.loads(cta_cli.exec_with_output("cta-admin --json version"))[0]["schedulerBackendName"] == "postgres"


@pytest.fixture(scope="session")
def cta_storage_class() -> str:
    return "cta_storage_class"


@pytest.fixture(scope="session")
def cta_default_tape_pool() -> str:
    # For now; don't change, because the populate_catalogue.sh script does not use this value (yet)
    return "ctasystest"


@pytest.fixture(scope="session")
def eos_workflow_dir(eos_mgm: EosMgmHost) -> Path:
    return eos_mgm.base_dir_path / "proc" / "cta" / "workflow"


@pytest.fixture(scope="session")
def cta_dir(disk_instance: DiskInstanceHost) -> Path:
    return disk_instance.base_dir_path / "cta"


# Very important that this is module scoped to ensure each test module gets it's own unique test directory
@pytest.fixture(scope="module")
def test_dir(cta_dir: Path, disk_instance: DiskInstanceHost, request: SubRequest) -> Path:
    module_name = Path(request.module.__file__).stem
    # Put the time in there so that it's easy from multiple runs to identify which one was last
    path = cta_dir / "tests" / f"{module_name}_{datetime.now():%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}"
    disk_instance.mkdir(path)
    return path


@pytest.fixture(scope="session")
def remote_scripts_dir() -> Path:
    # Note that this resolves relative to the current file
    return Path(__file__).parents[1] / "tests" / "remote_scripts"


#####################################################################################################################
# Hosts Initialisation
#####################################################################################################################


@pytest.fixture(scope="session")
def namespace(request: SubRequest) -> Optional[str]:
    return request.config.getoption("--namespace", default=None)


@pytest.fixture(scope="session")
def connection_config(request: SubRequest) -> Optional[Path]:
    return request.config.getoption("--connection-config", default=None)


@pytest.fixture(scope="session")
def env(connection_config: Optional[Path], namespace: Optional[str]) -> TestEnv:
    """Gives all the tests access to the different hosts (cli, frontend, taped, etc)"""
    if namespace and connection_config:
        raise pytest.UsageError("Only one of --namespace or --connection-config can be provided, not both")

    if namespace is None and connection_config is None:
        raise pytest.UsageError(
            "Missing mandatory argument: one of --namespace or --connection-config must be provided"
        )

    if namespace is not None:
        # No connection configuration provided, so assume everything is running in a cluster
        return TestEnv.from_namespace(namespace)
    assert connection_config is not None
    return TestEnv.from_config(connection_config)


#####################################################################################################################
# Hosts
#####################################################################################################################

# Some tests we skip, others we fail if the hosts are not present


@pytest.fixture(scope="session")
def eos_client(env: TestEnv) -> EosClientHost:
    if not env.eos_client:
        pytest.skip("This test requires at least one EOS client")
    return env.eos_client[0]


@pytest.fixture(scope="session")
def disk_client(env: TestEnv) -> DiskClientHost:
    return env.disk_client[0]


@pytest.fixture(scope="session")
def eos_mgm(env: TestEnv) -> EosMgmHost:
    if not env.eos_mgm:
        pytest.skip("This test requires an EOS deployment")
    return env.eos_mgm[0]


@pytest.fixture(scope="session")
def disk_instance(env: TestEnv) -> DiskInstanceHost:
    return env.disk_instance[0]


@pytest.fixture(scope="session")
def cta_taped(env: TestEnv) -> CtaTapedHost:
    return env.cta_taped[0]


@pytest.fixture(scope="session")
def cta_rmcd(env: TestEnv) -> CtaRmcdHost:
    return env.cta_rmcd[0]


@pytest.fixture(scope="session")
def cta_maintd(env: TestEnv) -> CtaMaintdHost:
    return env.cta_maintd[0]


@pytest.fixture(scope="session")
def cta_admin_api(env: TestEnv) -> CtaAdminApiHost:
    return env.cta_admin_api[0]


@pytest.fixture(scope="session")
def cta_workflow_api(env: TestEnv) -> CtaWorkflowApiHost:
    return env.cta_workflow_api[0]


@pytest.fixture(scope="session")
def cta_cli(env: TestEnv) -> CtaCliHost:
    return env.cta_cli[0]


@pytest.fixture(scope="session")
def disk_instance_name(disk_instance: DiskInstanceHost) -> str:
    return disk_instance.instance_name
