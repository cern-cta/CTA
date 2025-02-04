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
        print(f"Setting up namespace: {namespace}")
        return TestEnv.fromNamespace(namespace)
    else:
        print(f"Using connection config: {connection_config}")
        return TestEnv.fromConfig(connection_config)

def pytest_addoption(parser):
    parser.addoption("--namespace", action="store", help="Namespace for tests")
    parser.addoption("--connection-config", action="store", help="A yaml connection file specifying how to connect to each host")
    parser.addoption("--no-setup", action="store_true", help="Skip the execution of test_setup")

def pytest_collection_modifyitems(config, items):
    """Automatically include test_setup.py every time pytest runs."""
    if not config.getoption("--no-setup"):
        setup_path = Path("system_tests/test_setup.py").resolve()
        if not setup_path.exists():
            raise FileNotFoundError(f"Required test suite '{setup_path}' not found!")
        # Import test_setup.py to ensure pytest collects its tests
        setup_module = pytest.Module.from_parent(items[0].session, path=setup_path)
        setup_module.collect()
        # Insert test_setup at the beginning to ensure it is executed first
        items[:0] = setup_module.collect()
