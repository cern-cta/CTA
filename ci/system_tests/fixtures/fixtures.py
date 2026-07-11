# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest

import json
import shutil
from datetime import datetime
from pathlib import Path
import uuid

from ..helpers.test_env import TestEnv
from ..helpers.hosts import (
    CtaCliHost,
    CtaAdminApiHost,
    CtaWorkflowApiHost,
    CtaMaintdHost,
    CtaRmcdHost,
    CtaTapedHost,
    EosClientHost,
    EosMgmHost,
    DiskInstanceHost,
    DiskClientHost,
)

# This file could be split into multiple files eventually when necessary

#####################################################################################################################
# General
#####################################################################################################################


@pytest.fixture(autouse=True)
def make_tests_look_pretty(request):
    """The only purpose of this fixture is to make the test output easier to read
    in particular by more clearly visually separating different test cases
    """
    terminal_writer = request.config.get_terminal_writer()
    terminal_width = shutil.get_terminal_size().columns

    # construct the magic separators
    test_name = request.node.name
    test_title = f" {test_name} "  # leave 2 spaces around the name
    side = (terminal_width - len(test_title)) // 2
    line = "=" * side + test_title + "=" * (terminal_width - side - len(test_title))
    separator: str = "—" * terminal_width

    terminal_writer.write("\n" + line + "\n\n", cyan=True, bold=True)
    yield
    terminal_writer.write(f"\n\n{separator}", cyan=True)


@pytest.fixture(scope="session")
def error_whitelist() -> set[str]:
    """Mutable whitelist that individual test cases can add errors to"""
    whitelist = set()  # mutable whitelist shared between all tests
    return whitelist


@pytest.fixture(scope="session")
def project_json():
    project_json_path = (Path(__file__).resolve().parent / ".." / ".." / "project.json").resolve()
    return json.loads(project_json_path.read_text(encoding="utf-8"))


#####################################################################################################################
# Test-specific
#####################################################################################################################


@pytest.fixture(scope="session")
def krb5_realm(request) -> str:
    """Kerberos realm used in the tests"""
    return request.config.test_config["tests"]["krb5_realm"]


@pytest.fixture(scope="session")
def postgres_scheduler_enabled(cta_cli) -> str:
    # Not very robust; something to improve in the future. Maybe with a label on the entire cluster
    return json.loads(cta_cli.exec_with_output("cta-admin --json version"))["schedulerBackendName"] == "postgres"


@pytest.fixture(scope="session")
def cta_storage_class():
    return "cta_storage_class"


@pytest.fixture(scope="session")
def cta_default_tape_pool():
    # For now; don't change, because the populate_catalogue.sh script does not use this value (yet)
    return "ctasystest"


@pytest.fixture(scope="session")
def eos_workflow_dir(eos_mgm):
    return eos_mgm.base_dir_path / "proc" / "cta" / "workflow"


@pytest.fixture(scope="session")
def cta_dir(disk_instance):
    return disk_instance.base_dir_path / "cta"


# Very important that this is module scoped to ensure each test module gets it's own unique test directory
@pytest.fixture(scope="module")
def test_dir(cta_dir, disk_instance, request):
    module_name = Path(request.module.__file__).stem
    # Put the time in there so that it's easy from multiple runs to identify which one was last
    path = cta_dir / "tests" / f"{module_name}_{datetime.now():%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}"
    disk_instance.mkdir(path)
    return path


@pytest.fixture(scope="session")
def remote_scripts_dir():
    # Note that this resolves relative to the current file
    return Path(__file__).parents[1] / "tests" / "remote_scripts"


#####################################################################################################################
# Hosts Initialisation
#####################################################################################################################


@pytest.fixture(scope="session")
def namespace(request):
    return request.config.getoption("--namespace", default=None)


@pytest.fixture(scope="session")
def connection_config(request):
    return request.config.getoption("--connection-config", default=None)


@pytest.fixture(scope="session")
def env(connection_config, namespace) -> TestEnv:
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
    else:
        return TestEnv.from_config(connection_config)


#####################################################################################################################
# Hosts
#####################################################################################################################

# Some tests we skip, others we fail if the hosts are not present


@pytest.fixture(scope="session")
def eos_client(env) -> EosClientHost:
    if not env.eos_client:
        pytest.skip("This test requires at least one EOS client")
    return env.eos_client[0]


@pytest.fixture(scope="session")
def disk_client(env) -> DiskClientHost:
    return env.disk_client[0]


@pytest.fixture(scope="session")
def eos_mgm(env) -> EosMgmHost:
    if not env.eos_mgm:
        pytest.skip("This test requires an EOS deployment")
    return env.eos_mgm[0]


@pytest.fixture(scope="session")
def disk_instance(env) -> DiskInstanceHost:
    return env.disk_instance[0]


@pytest.fixture(scope="session")
def cta_taped(env) -> CtaTapedHost:
    return env.cta_taped[0]


@pytest.fixture(scope="session")
def cta_rmcd(env) -> CtaRmcdHost:
    return env.cta_rmcd[0]


@pytest.fixture(scope="session")
def cta_maintd(env) -> CtaMaintdHost:
    return env.cta_maintd[0]


@pytest.fixture(scope="session")
def cta_admin_api(env) -> CtaAdminApiHost:
    return env.cta_admin_api[0]


@pytest.fixture(scope="session")
def cta_workflow_api(env) -> CtaWorkflowApiHost:
    return env.cta_workflow_api[0]


@pytest.fixture(scope="session")
def cta_cli(env) -> CtaCliHost:
    return env.cta_cli[0]


@pytest.fixture(scope="session")
def disk_instance_name(disk_instance) -> str:
    return disk_instance.instance_name
