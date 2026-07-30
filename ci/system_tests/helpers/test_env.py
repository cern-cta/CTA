# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import subprocess
from collections.abc import Sequence
from pathlib import Path

from kubernetes import client, config
from kubernetes.client import ApiException, V1Pod

from .connections.k8s_connection import K8sConnection
from .connections.remote_connection import RemoteConnection
from .connections.ssh_connection import SSHConnection
from .hosts.cta_admin_api_host import CtaAdminApiHost
from .hosts.cta_cli_host import CtaCliHost
from .hosts.cta_maintd_host import CtaMaintdHost
from .hosts.cta_rmcd_host import CtaRmcdHost
from .hosts.cta_taped_host import CtaTapedHost
from .hosts.cta_workflow_api_host import CtaWorkflowApiHost
from .hosts.disk.disk_client_host import DiskClientHost
from .hosts.disk.disk_instance_host import DiskInstanceHost
from .hosts.disk.eos_client_host import EosClientHost
from .hosts.disk.eos_mgm_host import EosMgmHost


class TestEnv:
    def __init__(
        self,
        cta_cli_conns: Sequence[RemoteConnection] = [],
        cta_admin_api_conns: Sequence[RemoteConnection] = [],
        cta_workflow_api_conns: Sequence[RemoteConnection] = [],
        cta_rmcd_conns: Sequence[RemoteConnection] = [],
        cta_taped_conns: Sequence[RemoteConnection] = [],
        cta_maintd_conns: Sequence[RemoteConnection] = [],
        eos_client_conns: Sequence[RemoteConnection] = [],
        eos_mgm_conns: Sequence[RemoteConnection] = [],
    ) -> None:
        self.cta_cli: Sequence[CtaCliHost] = [CtaCliHost(conn) for conn in cta_cli_conns]
        self.cta_admin_api: Sequence[CtaAdminApiHost] = [CtaAdminApiHost(conn) for conn in cta_admin_api_conns]
        self.cta_workflow_api: Sequence[CtaWorkflowApiHost] = [
            CtaWorkflowApiHost(conn) for conn in cta_workflow_api_conns
        ]
        self.cta_rmcd: Sequence[CtaRmcdHost] = [CtaRmcdHost(conn) for conn in cta_rmcd_conns]
        self.cta_maintd: Sequence[CtaMaintdHost] = [CtaMaintdHost(conn) for conn in cta_maintd_conns]
        self.cta_taped: Sequence[CtaTapedHost] = [CtaTapedHost(conn) for conn in cta_taped_conns]
        self.eos_mgm: Sequence[EosMgmHost] = [EosMgmHost(conn) for conn in eos_mgm_conns]
        self.eos_client: Sequence[EosClientHost] = [EosClientHost(conn) for conn in eos_client_conns]
        # These should all fall under DiskInstanceHost and DiskClientHost
        self.disk_instance: Sequence[DiskInstanceHost] = self.eos_mgm  # + self.dcache
        self.disk_client: Sequence[DiskClientHost] = self.eos_client  # + self.dcache_client

    # Mostly a convenience function that is arguably not very clean, but that is for later
    @staticmethod
    def exec_local(
        command: str,
        capture_output: bool = False,
        throw_on_failure: bool = True,
    ) -> subprocess.CompletedProcess[bytes]:
        full_command = f'bash -c "{command}"'
        result = subprocess.run(full_command, shell=True, capture_output=capture_output)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(
                f"local exec of {full_command} failed with exit code {result.returncode}: {result.stderr}"
            )
        return result

    @staticmethod
    def get_k8s_connections_by_selector(
        namespace: str,
        selector: str,
        container_value: str,
    ) -> list[K8sConnection]:
        """
        Returns a list of K8sConnection objects.
        """
        core = client.CoreV1Api()

        pods = core.list_namespaced_pod(
            namespace=namespace,
            label_selector=selector,
        )
        pod_items: list[V1Pod] = pods.items

        def pod_name(pod: V1Pod) -> str:
            if pod.metadata is None:
                return ""
            name: str = pod.metadata.name
            return name or ""

        pod_items.sort(key=pod_name)

        connections: list[K8sConnection] = []
        for ordinal, pod in enumerate(pod_items):
            if pod.spec is None:
                continue
            for c in pod.spec.containers or []:
                cname = c.name or ""
                if container_value in cname or not container_value:
                    connections.append(K8sConnection(namespace, selector, cname, ordinal))

        print(f"K8s {selector}: {len(connections)}")

        return connections

    @staticmethod
    def from_namespace(namespace: str) -> "TestEnv":
        config.load_kube_config()
        core = client.CoreV1Api()
        try:
            core.read_namespace(name=namespace)
        except ApiException as e:
            raise RuntimeError(f"Failed to query namespace {namespace}: {e}") from e
        return TestEnv(
            # Our "cta-client" should actually be an eos-client. However, the current bash test suite mixes these
            # concepts
            # Something to be changed once we move them over....
            cta_cli_conns=TestEnv.get_k8s_connections_by_selector(
                namespace, "app.kubernetes.io/component=cli", "cta-cli"
            ),
            cta_admin_api_conns=TestEnv.get_k8s_connections_by_selector(
                namespace, "app.kubernetes.io/component=frontend-admin", "cta-frontend"
            ),
            cta_workflow_api_conns=TestEnv.get_k8s_connections_by_selector(
                namespace, "app.kubernetes.io/component=frontend-wfe", "cta-frontend"
            ),
            cta_rmcd_conns=TestEnv.get_k8s_connections_by_selector(
                namespace, "app.kubernetes.io/component=rmcd", "cta-rmcd"
            ),
            cta_maintd_conns=TestEnv.get_k8s_connections_by_selector(
                namespace, "app.kubernetes.io/component=maintd", "cta-maintd"
            ),
            cta_taped_conns=TestEnv.get_k8s_connections_by_selector(
                namespace, "app.kubernetes.io/component=taped", "cta-taped"
            ),
            eos_client_conns=TestEnv.get_k8s_connections_by_selector(
                namespace, "app.kubernetes.io/component=client", "client"
            ),
            eos_mgm_conns=TestEnv.get_k8s_connections_by_selector(namespace, "app.kubernetes.io/name=mgm", "mgm"),
        )

    @staticmethod
    def from_config(path: Path) -> "TestEnv":
        """
        Expects a path to a yaml file containing for each host how to connect. For example:

        eos_client:
          - k8s:
              namespace: dev
              selector: app.kubernetes.io/component=client
              container: client
        cta_taped:
          - ssh:
              user: root
              host: tpsrv420
        """
        try:
            import yaml
        except ImportError as error:
            raise RuntimeError("Install pyyaml to use TestEnv.from_config()") from error
        with path.open() as f:
            connection_config: dict[str, list[dict[str, dict[str, str]]]] = yaml.safe_load(f)

        def create_connections(
            config_data: dict[str, list[dict[str, dict[str, str]]]],
            host: str,
        ) -> list[RemoteConnection]:
            """Creates a list of RemoteConnection objects for a given host."""
            if host not in config_data:
                raise ValueError(f"Invalid connection configuration: missing host {host}")

            connections: list[RemoteConnection] = []
            for connection in config_data[host]:  # Iterate over the list of connection configurations
                if "k8s" in connection:
                    k8s = connection["k8s"]
                    connections.extend(
                        TestEnv.get_k8s_connections_by_selector(k8s["namespace"], k8s["selector"], k8s["container"]),
                    )
                elif "ssh" in connection:
                    ssh = connection["ssh"]
                    connections.append(SSHConnection(user=ssh["user"], host=ssh["host"]))
                else:
                    raise ValueError("Invalid connection configuration: must specify either 'k8s' or 'ssh'")

            return connections

        return TestEnv(
            cta_cli_conns=create_connections(connection_config, "cta_cli"),
            cta_admin_api_conns=create_connections(connection_config, "cta_admin_api"),
            cta_workflow_api_conns=create_connections(connection_config, "cta_workflow_api"),
            cta_rmcd_conns=create_connections(connection_config, "cta_rmcd"),
            cta_taped_conns=create_connections(connection_config, "cta_taped"),
            eos_client_conns=create_connections(connection_config, "eos_client"),
            eos_mgm_conns=create_connections(connection_config, "eos_mgm"),
        )
