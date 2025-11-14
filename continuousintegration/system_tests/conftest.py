import pytest
from pathlib import Path
from .helpers.test_env import TestEnv

# In case we ever want to support ssh, simply add another option and do some parsing here
# Return a different environment in that case
@pytest.fixture(scope="session", autouse=True)
def env(request):
    namespace = request.config.getoption("--namespace", default=None)
    connection_config = request.config.getoption("--connection-config", default=None)

    if namespace and connection_config:
        pytest.exit("ERROR: Only one of --namespace or --connection-config can be provided, not both.", returncode=1)

    if namespace is None and connection_config is None:
        pytest.exit("ERROR: Missing mandatory argument: one of --namespace or --connection-config must be provided", returncode=1)

    if connection_config is None:
        # No connection configuration provided, so assume everything is running in a cluster
        return TestEnv.fromNamespace(namespace)
    else:
        return TestEnv.fromConfig(connection_config)

def pytest_addoption(parser):
    parser.addoption("--namespace", action="store", help="Namespace for tests")
    parser.addoption("--connection-config", action="store", help="A yaml connection file specifying how to connect to each host")
    parser.addoption("--no-setup", action="store_true", help="Skip the execution of test_setup")
    parser.addoption("--no-teardown", action="store_true", help="Skip the execution of test_teardown")
    parser.addoption("--clean-start", action="store_true", help="Run the teardown before starting the tests to ensure a clean start")

def is_test_in_items(test_path: str, items):
    resolved_test_path = Path(test_path).resolve()
    if not resolved_test_path.exists():
        raise FileNotFoundError(f"Test suite '{resolved_test_path}' not found!")
    return any(str(resolved_test_path) == str(item.path) for item in items)


def add_test_into_existing_collection(test_path: str, items, prepend: bool = False, allow_duplicate: bool = False):
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
    """Automatically include some tests every time pytest runs. These tests can be skipped"""
    setup_script: str = "tests/test_setup.py"
    error_script: str = "tests/test_errors.py"
    teardown_script: str = "tests/test_teardown.py"

    # Always check for errors after the run
    add_test_into_existing_collection(error_script, items, prepend=False)
    # Always do a setup of CTA before the tests start (unless skipped)
    if not is_test_in_items(setup_script, items) and not config.getoption("--no-setup"):
        # Only add it if it wasn't manually added by the user
        add_test_into_existing_collection(setup_script, items, prepend=True)
    # Always do a teardown of CTA after the tests finnish (unless skipped)
    if not is_test_in_items(setup_script, items) and not config.getoption("--no-teardown"):
        # Only add it if it wasn't manually added by the user
        add_test_into_existing_collection(teardown_script, items, prepend=False)
    # Do the reset before the tests start. Useful when rerunning the tests multiple times on the same instance
    if config.getoption("--clean-start"):
        add_test_into_existing_collection(teardown_script, items, prepend=True, allow_duplicate=True)
