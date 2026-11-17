# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Check deployed connection policy before system tests can modify state."""

import pytest
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from kubernetes.config.config_exception import ConfigException
from urllib3.exceptions import HTTPError


def require_unprotected_connections(namespace: str) -> None:
    try:
        config.load_kube_config()
        policy = (
            client.CoreV1Api()
            .read_namespaced_config_map("cta-connection-protection", namespace, _request_timeout=30)
            .data
        )
    except (OSError, ApiException, ConfigException, HTTPError, ValueError) as error:
        raise pytest.UsageError(f"Cannot verify connection protection in {namespace}; refusing system tests") from error
    if not isinstance(policy, dict) or any(
        policy.get(db) not in ("true", "false") for db in ("catalogue", "scheduler")
    ):
        raise pytest.UsageError(f"Invalid connection protection in {namespace}; refusing system tests")
    for database in ("catalogue", "scheduler"):
        if policy[database] == "true":
            raise pytest.UsageError(f"System tests are blocked: {database} connection is protected in {namespace}")
