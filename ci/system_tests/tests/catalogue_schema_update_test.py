# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from pathlib import Path
import pytest


#####################################################################################################################
# Helpers
#####################################################################################################################


@pytest.fixture(scope="session")
def catalogue_from_version(project_json):
    supported_major_versions = project_json["supportedCatalogueVersions"]
    current_major_version = project_json["catalogueVersion"]
    return project_json["catalogueVersion"]


@pytest.fixture(scope="session")
def catalogue_to_version(project_json):
    # prev_catalogue_schema_version=$(jq .supportedCatalogueVersions[] ${project_json_file} | grep -v $catalogue_schema_version | head -1)
    return project_json["catalogueVersion"]


#####################################################################################################################
# Tests
#####################################################################################################################


def test_hosts_present_liquibase(env):
    assert len(env.cta_frontend) > 0
    assert len(env.cta_cli) > 0


def test_catalogue_version_is_from_version(env, catalogue_from_version):
    # First check the current version is equal to the "from" version
    assert (
        env.cta_frontend[0].get_schema_version() == catalogue_from_version
    ), 'Catalogue version should be equal to the "from" version before any updates'


def test_create_catalogue_updater(env, project_json, catalogue_from_version, catalogue_to_version):
    # This is pretty disgusting but for now this will do
    # If the configmap generation would be done through Helm the file in question needs to be within the chart
    defaultPlatform = project_json["dev"]["defaultPlatform"]
    yum_repos_file = (
        Path(__file__).resolve().parent / ".." / ".." / "docker" / defaultPlatform / "etc" / "yum.repos.d-internal"
    ).resolve()
    env.execLocal(f"kubectl -n {NAMESPACE} create configmap yum.repos.d-config --from-file={yum_repos_file}")

    env.execLocal(
        f"helm install catalogue-updater ../helm/catalogue-updater --namespace {NAMESPACE} \
                                                         --set catalogueSourceVersion={catalogue_from_version} \
                                                         --set catalogueDestinationVersion={catalogue_to_version} \
                                                         --wait --timeout 2m"
    )
    env.catalogue_updater = ...


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
