# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from pathlib import Path
import pytest
from dataclasses import dataclass
from ..helpers.connections.k8s_connection import K8sConnection
from ..helpers.hosts.remote_host import RemoteHost


#####################################################################################################################
# Helpers
#####################################################################################################################


@dataclass
class CatalogueSchemaUpdateParams:
    schema_checkout_ref: str


@pytest.fixture(scope="class")
def catalogue_schema_update_params(request):
    catalogue_schema_update_config = request.config.test_config["tests"]["catalogue_schema_update"]
    return CatalogueSchemaUpdateParams(schema_checkout_ref=catalogue_schema_update_config["schema_checkout_ref"])


@pytest.fixture(scope="class")
def catalogue_from_version(project_json):
    supported_major_versions = project_json["supportedCatalogueVersions"]
    supported_major_versions.sort()
    # Sorted from low to high, the "from" version is the lowest major version + ".0"
    return str(supported_major_versions[0]) + ".0"


@pytest.fixture(scope="class")
def catalogue_to_version(project_json):
    supported_major_versions = project_json["supportedCatalogueVersions"]
    supported_major_versions.sort()
    # Sorted from low to high, the "to" version is the highest major version + ".0"
    return str(supported_major_versions[-1]) + ".0"


@pytest.fixture(scope="class")
def namespace(request):
    return request.config.getoption("--namespace", default=None)


#####################################################################################################################
# Tests
#####################################################################################################################


def test_hosts_present_liquibase(env):
    assert len(env.cta_frontend) > 0


def test_multiple_versions_supported(project_json):
    assert (
        len(project_json["supportedCatalogueVersions"]) > 1
    ), "In order to test a catalogue schema update, CTA must be compatible with at least 2 catalogue schema versions"


def test_catalogue_version_is_from_version(env, catalogue_from_version):
    # First check the current version is equal to the "from" version
    assert (
        env.cta_frontend[0].get_schema_version() == catalogue_from_version
    ), 'Catalogue version should be equal to the "from" version before any updates'


def test_init_catalogue_updater(
    env, project_json, catalogue_from_version, catalogue_to_version, catalogue_schema_update_params, namespace
):
    # Just to note, this entire method is hacky and should be rewritten at some point. Suggestions welcome...
    # A few of the problems with it:
    # - It makes plain kubectl calls and therefore assumes things are running on a Kubernetes cluster
    # - It makes a call to helm, therefore requiring helm and assuming Kubernetes (again)
    # - It has to create a configmap based on a file somewhere else in the repo

    # If the configmap generation would need to be done through Helm the file in question needs to be within the chart
    defaultPlatform = project_json["dev"]["defaultPlatform"]
    yum_repos_file = (
        Path(__file__).resolve().parent / ".." / ".." / "docker" / defaultPlatform / "etc" / "yum.repos.d-internal"
    ).resolve()
    env.execLocal(f"kubectl -n {namespace} create configmap yum.repos.d-config --from-file={yum_repos_file}")

    print(f"Catalogue source version: {catalogue_from_version}")
    print(f"Catalogue destination version: {catalogue_to_version}")
    print("Install catalogue-updater chart...")

    extraFlags = ""
    if catalogue_schema_update_params.schema_checkout_ref:
        extraFlags = f"--set extraFlags='--schema-checkout-ref {catalogue_schema_update_params.schema_checkout_ref}'"

    env.execLocal(
        f"helm install catalogue-updater ../orchestration/helm/catalogue-updater --namespace {namespace} \
                                                         --set catalogueSourceVersion={catalogue_from_version} \
                                                         --set catalogueDestinationVersion={catalogue_to_version} \
                                                        {extraFlags} --wait --timeout 2m"
    )
    # Ensure we can exec into the container from here on out
    env.catalogue_updater = RemoteHost(K8sConnection(namespace, "liquibase-update", "liquibase-update"))


def test_tag_liquibase(env):
    env.catalogue_updater.exec('/launch_liquibase.sh "tag --tag=test_update"')


def test_liquibase_update(env, catalogue_to_version):
    env.catalogue_updater.exec("/launch_liquibase.sh update")
    # Now the current version should be equal to the "to" version
    assert (
        env.cta_frontend[0].get_schema_version() == catalogue_to_version
    ), 'Catalogue version should be equal to the "to" version after rollback'


def test_liquibase_rollback(env, catalogue_from_version):
    env.catalogue_updater.exec('/launch_liquibase.sh "rollback --tag=test_update"')
    # Check the current version is equal to the "from" version again
    assert (
        env.cta_frontend[0].get_schema_version() == catalogue_from_version
    ), 'Catalogue version should be equal to the "from" version after rollback'


def test_cleanup_catalogue_updater(env, namespace):
    env.catalogue_updater = None
    env.execLocal(f"helm uninstall catalogue-updater --namespace {namespace}")
    env.execLocal(f"kubectl -n {namespace} delete configmap yum.repos.d-config")
