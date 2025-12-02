import pytest
from pathlib import Path
from .helpers.test_env import TestEnv
from .helpers.hosts.disk.disk_instance_host import DiskInstanceImplementation
import shutil

#####################################################################################################################
# General/common fixtures
#####################################################################################################################

def get_test_env(config):
    namespace = config.getoption("--namespace", default=None)
    connection_config = config.getoption("--connection-config", default=None)

    if namespace and connection_config:
        pytest.exit("ERROR: Only one of --namespace or --connection-config can be provided, not both.", returncode=1)

    if namespace is None and connection_config is None:
        pytest.exit(
            "ERROR: Missing mandatory argument: one of --namespace or --connection-config must be provided",
            returncode=1,
        )

    if connection_config is None:
        # No connection configuration provided, so assume everything is running in a cluster
        return TestEnv.fromNamespace(namespace)
    else:
        return TestEnv.fromConfig(connection_config)


# This is how all the tests get access to the different hosts (cli, frontend, taped, etc)
@pytest.fixture(scope="session", autouse=True)
def env(request):
    return get_test_env(request.config)


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


# Mutable whitelist that individual test cases can add errors to
@pytest.fixture(scope="session")
def error_whitelist(request):
    whitelist = set()  # mutable whitelist shared between all tests
    return whitelist


@pytest.fixture()
def krb5_realm(request):
    return request.config.getoption("--krb5-realm")


#####################################################################################################################
# Commandline options
#####################################################################################################################


def pytest_addoption(parser):
    parser.addoption("--namespace", action="store", help="Namespace for tests")
    parser.addoption(
        "--connection-config", action="store", help="A yaml connection file specifying how to connect to each host"
    )
    parser.addoption("--no-setup", action="store_true", help="Skip the execution of setup_test")
    parser.addoption("--no-teardown", action="store_true", help="Skip the execution of teardown_test")
    parser.addoption(
        "--clean-start", action="store_true", help="Run the teardown before starting the tests to ensure a clean start"
    )

    # Test specific options
    parser.addoption(
        "--krb5-realm", type=str, default="TEST.CTA", help="Kerberos realm to use for cta-admin/eos commands"
    )
    parser.addoption("--stress-num-dirs", type=int, default=10, help="Number of directories to use for the stress test")
    parser.addoption(
        "--stress-num-files-per-dir",
        type=int,
        default=1000,
        help="Number of files to put in each directory for the stress test",
    )
    parser.addoption(
        "--stress-file-size", type=int, default=512, help="Size of the files in bytes to use for the stress test"
    )


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
        raise RuntimeError(f"No tests found")
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


def pytest_collection_modifyitems(config, items):
    # Always check for errors after the run
    add_test_into_existing_collection("tests/teardown/error_test.py", items, prepend=False)

    if not config.getoption("--no-setup"):
        add_test_into_existing_collection("tests/setup/setup_cta_test.py", items, prepend=True)
        # If EOS is not the used disk system, these will be filtered out later
        add_test_into_existing_collection("tests/setup/setup_eos_test.py", items, prepend=True)
        # add_test_into_existing_collection("tests/setup/setup_dcache_test.py", items, prepend=True)

    if not config.getoption("--no-teardown"):
        add_test_into_existing_collection("tests/teardown/cleanup_cta_test.py", items, prepend=False)
        # If EOS is not the used disk system, these will be filtered out later
        add_test_into_existing_collection("tests/teardown/cleanup_eos_test.py", items, prepend=False)
        # add_test_into_existing_collection("tests/teardown/cleanup_dcache_test.py", items, prepend=True)

    # Do the reset before the tests start.
    # Useful when rerunning the tests multiple times on the same instance and it wasn't properly cleaned up
    if config.getoption("--clean-start"):
        add_test_into_existing_collection("tests/teardown/cleanup_cta_test.py", items, prepend=True, allow_duplicate=True)
        # If EOS is not the used disk system, these will be filtered out later
        add_test_into_existing_collection("tests/teardown/cleanup_eos_test.py", items, prepend=True, allow_duplicate=True)
        # add_test_into_existing_collection("tests/teardown/cleanup_dcache_test.py", items, prepend=True)

    # Now figure out which disk instance are present in the test setup, so that we can skip
    # any marked tests for disk instances not in our environment
    test_env = get_test_env(config)
    present_disk_instances: list[str] = [di.implementation.label for di in test_env.disk_instance]
    all_disk_instances: list[str] = [e.label for e in DiskInstanceImplementation]
    skip_marks: list[str] = list(all_disk_instances - {present_disk_instances})
    # Skip all tests specific to disk instances not present
    for item in items:
      if any(mark in item.keywords for mark in skip_marks):
          item.add_marker(pytest.mark.skip(reason="Skipping test because it has a disabled mark"))
