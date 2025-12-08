import pytest
from pathlib import Path
from .helpers.test_env import TestEnv
from .helpers.hosts.disk.disk_instance_host import DiskInstanceImplementation
import shutil

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:
    import tomli as tomllib  # Python <3.11

#####################################################################################################################
# General/common fixtures
#####################################################################################################################


# The only purpose of this fixture is to make the test output easier to read
# in particular by more clearly visually separating different test cases
@pytest.fixture(autouse=True)
def make_tests_look_pretty(request):
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


# This is how all the tests get access to the different hosts (cli, frontend, taped, etc)
@pytest.fixture(scope="session", autouse=True)
def env(request):
    return request.config.env


# Mutable whitelist that individual test cases can add errors to
@pytest.fixture(scope="session")
def error_whitelist(request):
    whitelist = set()  # mutable whitelist shared between all tests
    return whitelist


# Kerberos realm used in the tests
@pytest.fixture()
def krb5_realm(request):
    return request.config.test_config["tests"]["krb5_realm"]


#####################################################################################################################
# Commandline options
#####################################################################################################################


def create_test_env_from_commandline_options(config):
    namespace = config.getoption("--namespace", default=None)
    connection_config = config.getoption("--connection-config", default=None)

    if namespace and connection_config:
        raise pytest.UsageError("Only one of --namespace or --connection-config can be provided, not both")

    if namespace is None and connection_config is None:
        raise pytest.UsageError(
            "Missing mandatory argument: one of --namespace or --connection-config must be provided"
        )

    if connection_config is None:
        # No connection configuration provided, so assume everything is running in a cluster
        return TestEnv.fromNamespace(namespace)
    else:
        return TestEnv.fromConfig(connection_config)


# Pytest hook that allows for adding custom commandline arguments
def pytest_addoption(parser):
    parser.addoption("--namespace", action="store", help="Namespace for tests")
    parser.addoption(
        "--connection-config", action="store", help="A yaml connection file specifying how to connect to each host"
    )
    parser.addoption("--no-setup", action="store_true", help="Skip the execution of the setup tests")
    parser.addoption("--no-cleanup", action="store_true", help="Skip the execution of the cleanup tests")
    parser.addoption(
        "--cleanup-first", action="store_true", help="Run the cleanup before starting the tests to ensure a clean start"
    )
    parser.addoption(
        "--test-config",
        type=str,
        default="config/test_params.toml",
        help="Path to the config file containing all test parameters",
    )


# Pytest hook that allows us to augment the config object with additional info after commandline parsing
def pytest_configure(config):
    config_path: str = config.getoption("--test-config")
    try:
        with open(config_path, "rb") as f:
            config.test_config = tomllib.load(f)
    except FileNotFoundError:
        raise pytest.UsageError(f"--test-config file not found: {config_path}")
    try:
        config.env = create_test_env_from_commandline_options(config)
    except Exception as e:
        raise pytest.UsageError(f"Failed to create test environment: {e}")



#####################################################################################################################
# Do some magic to automatically add setup and teardown to the test suite
#####################################################################################################################


def is_test_in_items(test_path: str, items):
    resolved_test_path = Path(test_path).resolve()
    if not resolved_test_path.exists():
        raise FileNotFoundError(f"Test suite '{resolved_test_path}' not found!")
    return any(str(resolved_test_path) == str(item.path) for item in items)


def add_test_into_existing_collection(test_path: str, items, prepend: bool = False, allow_duplicate: bool = False):
    if not items:
        raise RuntimeError("No tests found")
    resolved_test_path = Path(test_path).resolve()
    if not resolved_test_path.exists():
        raise FileNotFoundError(f"Required test suite '{resolved_test_path}' not found!")
    # Prevent duplicate registration unless explicitly allowed
    if not is_test_in_items(test_path, items) or allow_duplicate:
        # Import the test to ensure pytest collects its tests
        test_module = pytest.Module.from_parent(items[0].session, path=resolved_test_path)
        tests = test_module.collect()
        index = 0 if prepend else len(items)
        items[index:index] = tests


# Pytest hook that allows us to dynamically modify the set of tests being run
def pytest_collection_modifyitems(config, items):
    # Always check for errors after the run
    add_test_into_existing_collection("tests/cleanup/error_test.py", items, prepend=False)

    # Now figure out which disk instance are present in the test setup, so that we can skip
    # any marked tests for disk instances not in our environment
    present_disk_instances: list[DiskInstanceImplementation] = [di.implementation for di in config.env.disk_instance]

    if not config.getoption("--no-setup"):
        add_test_into_existing_collection("tests/setup/setup_cta_test.py", items, prepend=True)
        if DiskInstanceImplementation.EOS in present_disk_instances:
            add_test_into_existing_collection("tests/setup/setup_eos_test.py", items, prepend=True)
        if DiskInstanceImplementation.DCACHE in present_disk_instances:
            add_test_into_existing_collection("tests/setup/setup_dcache_test.py", items, prepend=True)

    if not config.getoption("--no-cleanup"):
        # Do the reset before the tests start.
        # Useful when rerunning the tests multiple times on the same instance and it wasn't properly cleaned up
        prepend = bool(config.getoption("--cleanup-first"))
        add_test_into_existing_collection("tests/cleanup/cleanup_cta_test.py", items, prepend=prepend)
        if DiskInstanceImplementation.EOS in present_disk_instances:
            add_test_into_existing_collection("tests/cleanup/cleanup_eos_test.py", items, prepend=prepend)
        if DiskInstanceImplementation.DCACHE in present_disk_instances:
            add_test_into_existing_collection("tests/cleanup/cleanup_dcache_test.py", items, prepend=prepend)

    all_disk_instances: list[DiskInstanceImplementation] = [e for e in DiskInstanceImplementation]
    skip_marks: list[str] = [e.label for e in (set(all_disk_instances) - set(present_disk_instances))]
    # Skip all tests specific to disk instances not present
    for item in items:
        if any(mark in item.keywords for mark in skip_marks):
            item.add_marker(
                pytest.mark.skip(reason="Skipping test because the disk instance required for this test is not present")
            )
