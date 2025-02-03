import pytest
import sys
import time
from stip.test_env import TestEnv

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

