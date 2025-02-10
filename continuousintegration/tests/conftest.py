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
    parser.addoption("--no-prerun-checks", action="store_true", help="Skip the execution of test_prerun")
    parser.addoption("--no-postrun-checks", action="store_true", help="Skip the execution of test_postrun")

def add_test_into_existing_collection(test_path: str, items, prepend: bool = False):
    resolved_test_path = Path(test_path).resolve()
    if not resolved_test_path.exists():
        raise FileNotFoundError(f"Required test suite '{resolved_test_path}' not found!")
    # Ensure test_setup.py is not already in items
    if not any(str(resolved_test_path) == str(item.path) for item in items):
        # Import the test to ensure pytest collects its tests
        test_module = pytest.Module.from_parent(items[0].session, path=resolved_test_path)
        tests = test_module.collect()
        index = 0 if prepend else len(items)
        items[index:index] = tests

def pytest_collection_modifyitems(config, items):
    """Automatically include some tests every time pytest runs. These tests can be skipped"""
    if not config.getoption("--no-setup"):
        add_test_into_existing_collection("system_tests/test_setup.py", items, prepend=True)
    if not config.getoption("--no-prerun-checks"):
        add_test_into_existing_collection("system_tests/test_prerun.py", items, prepend=True)
    if not config.getoption("--no-postrun-checks"):
        add_test_into_existing_collection("system_tests/test_postrun.py", items, prepend=False)
