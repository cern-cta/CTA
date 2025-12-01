import json
from typing import Any, List
import subprocess

from .hosts.cta.cta_cli_host import CtaCliHost
from .hosts.cta.cta_frontend_host import CtaFrontendHost
from .hosts.cta.cta_rmcd_host import CtaRmcdHost
from .hosts.cta.cta_taped_host import CtaTapedHost
from .hosts.disk.eos_client_host import EosClientHost
from .hosts.disk.eos_mgm_host import EosMgmHost
from .hosts.disk.disk_instance_host import DiskInstanceHost
from .hosts.disk.disk_client_host import DiskClientHost
from .connections.remote_connection import RemoteConnection
from .connections.k8s_connection import K8sConnection
from .connections.ssh_connection import SSHConnection


class TestEnv:
    def __init__(
        self,
        cta_cli_conns: list[RemoteConnection] = [],
        cta_frontend_conns: list[RemoteConnection] = [],
        cta_rmcd_conns: list[RemoteConnection] = [],
        cta_taped_conns: list[RemoteConnection] = [],
        eos_client_conns: list[RemoteConnection] = [],
        eos_mgm_conns: list[RemoteConnection] = [],
    ):
        self.cta_cli: list[CtaCliHost] = [CtaCliHost(conn) for conn in cta_cli_conns]
        self.cta_frontend: list[CtaFrontendHost] = [CtaFrontendHost(conn) for conn in cta_frontend_conns]
        self.cta_rmcd: list[CtaRmcdHost] = [CtaRmcdHost(conn) for conn in cta_rmcd_conns]
        self.cta_taped: list[CtaTapedHost] = [CtaTapedHost(conn) for conn in cta_taped_conns]
        self.eos_mgm: list[EosMgmHost] = [EosMgmHost(conn) for conn in eos_mgm_conns]
        self.eos_client: list[EosClientHost] = [EosClientHost(conn) for conn in eos_client_conns]
        # These should all fall under DiskInstanceHost and DiskClientHost
        self.disk_instance: list[DiskInstanceHost] = self.eos_mgm  # + self.dcache
        self.disk_client: list[DiskClientHost] = self.eos_client  # + self.dcache_client

    # Mostly a convenience function that is arguably not very clean, but that is for later
    @staticmethod
    def execLocal(command: str, capture_output=False, throw_on_failure=True):
        full_command = f'bash -c "{command}"'
        result = subprocess.run(full_command, shell=True, capture_output=capture_output)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(
                f"local exec of {full_command} failed with exit code {result.returncode}: {result.stderr}"
            )
        return result

    @staticmethod
    def get_k8s_connections_by_label(namespace: str, label_key: str, label_value: str, container_value: str = ""):
        """
        Returns a list of K8sConnection objects.
        label_value and container_value can be exact matches or partial matches.
        """
        list_pods_command = f"kubectl get pods -n {namespace} -o json"
        pods_json_raw = TestEnv.execLocal(list_pods_command, True)

        pods = json.loads(pods_json_raw.stdout.decode("utf-8"))
        connections: List[K8sConnection] = []

        for pod in pods.get("items", []):
            labels = pod.get("metadata", {}).get("labels", {})
            v = labels.get(label_key)

            if not v or label_value not in v:
                continue

            containers = pod.get("spec", {}).get("containers", [])
            for c in containers:
                cname = c.get("name", "")
                if container_value in cname or not container_value:
                    pod_name = pod["metadata"]["name"]
                    connections.append(K8sConnection(namespace, pod_name, cname))

        return connections

    @staticmethod
    def fromNamespace(namespace: str):
        return TestEnv(
            # Our "cta-client" should actually be an eos-client. However, the current test suite mixes these concepts
            # Something to be changed once we move them over....
            eos_client_conns=TestEnv.get_k8s_connections_by_label(namespace, "app.kubernetes.io/name", "cta-client"),
            cta_cli_conns=TestEnv.get_k8s_connections_by_label(namespace, "app.kubernetes.io/name", "cta-cli"),
            cta_frontend_conns=TestEnv.get_k8s_connections_by_label(
                namespace, "app.kubernetes.io/name", "cta-frontend"
            ),
            cta_rmcd_conns=TestEnv.get_k8s_connections_by_label(
                namespace, "app.kubernetes.io/name", "cta-tpsrv", "cta-rmcd"
            ),
            cta_taped_conns=TestEnv.get_k8s_connections_by_label(
                namespace, "app.kubernetes.io/name", "cta-tpsrv", "cta-taped"
            ),
            eos_mgm_conns=TestEnv.get_k8s_connections_by_label(namespace, "app.kubernetes.io/name", "mgm", "mgm"),
        )

    @staticmethod
    def fromConfig(path: str):
        """
        Expects a path to a yaml file containing for each host how to connect. For example:

        eosclient:
          - k8s:
              namespace: dev
              pod: client-0
              container: client
        ctafrontend:
          - ssh:
              user: root
              host: ctapreproductionfrontend
        """
        try:
            import yaml
        except ImportError:
            raise RuntimeError("Install pyyaml to use TestEnv.fromConfig()")
        with open(path, "r") as f:
            config = yaml.safe_load(f)

        def create_connections(config: Any, host: str) -> list:
            """Creates a list of RemoteConnection objects for a given host."""
            if host not in config:
                raise ValueError(f"Invalid connection configuration: missing host {host}")

            connections = []
            for connection in config[host]:  # Iterate over the list of connection configurations
                if "k8s" in connection:
                    k8s = connection["k8s"]
                    connections.append(
                        K8sConnection(namespace=k8s["namespace"], pod=k8s["pod"], container=k8s["container"])
                    )
                elif "ssh" in connection:
                    ssh = connection["ssh"]
                    connections.append(SSHConnection(user=ssh["user"], host=ssh["host"]))
                else:
                    raise ValueError("Invalid connection configuration: must specify either 'k8s' or 'ssh'")

            return connections

        return TestEnv(
            cta_cli_conns=create_connections(config, "ctacli"),
            cta_frontend_conns=create_connections(config, "ctafrontend"),
            cta_rmcd_conns=create_connections(config, "ctarmcd"),
            cta_taped_conns=create_connections(config, "ctataped"),
            eos_client_conns=create_connections(config, "eosclient"),
            eos_mgm_conns=create_connections(config, "eosmgm"),
        )
