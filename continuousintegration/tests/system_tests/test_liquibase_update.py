import pytest
import os

from ..helpers.hosts.remote_host import RemoteHost
from ..helpers.connections.k8s_connection import K8sConnection

# TODO: this is incomplete

@pytest.fixture(scope="module")
def liquibase_host(request):
    namespace = request.config.getoption("--namespace", default=None)
    if not namespace:
        pytest.exit("ERROR: The liquibase_host fixture can only be used when a namespace is provided", returncode=1)
    return RemoteHost(K8sConnection(namespace, "liquibase", "liquibase"))

@pytest.fixture
def destination_schema_version(env):
    ...

@pytest.fixture
def source_catalogue_version(env):
    ...

def test_current_schema_version_is_previous(env, source_catalogue_version):
    assert env.ctafrontend[0].get_schema_version() == source_catalogue_version

def test_install_catalogue_updater(env, source_catalogue_version, destination_schema_version):
    yum_repos_file = os.path.realpath("../../docker/alma9/etc/yum.repos.d")
    env.execLocal(f"kubectl -n {env.namespace} create configmap yum.repos.d-config --from-file=${yum_repos_file}")
    env.execLocal(f"helm install catalogue-updater ../../orchestration/helm/catalogue-updater --namespace {env.namespace} \
                                                            --set catalogueSourceVersion={source_catalogue_version} \
                                                            --set catalogueDestinationVersion={destination_schema_version} \
                                                            --wait --timeout 2m")

def test_liquibase_update(env, liquibase_host, destination_schema_version):
    liquibase_host.exec("/launch_liquibase.sh \"tag --tag=test_update\"")
    liquibase_host.exec("/launch_liquibase.sh update")
    assert env.ctafrontend[0].get_schema_version() == destination_schema_version

def test_liquibase_rollback(env, liquibase_host, source_catalogue_version):
    liquibase_host.exec("/launch_liquibase.sh \"rollback --tag=test_update\"")
    assert env.ctafrontend[0].get_schema_version() == source_catalogue_version
