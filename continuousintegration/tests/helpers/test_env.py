import yaml
from typing import Any
import subprocess

from .hosts.client_host import ClientHost
from .hosts.cta_cli_host import CtaCliHost
from .hosts.cta_frontend_host import CtaFrontendHost
from .hosts.cta_rmcd_host import CtaRmcdHost
from .hosts.cta_taped_host import CtaTapedHost
from .hosts.eos_mgm_host import EosMgmHost
from .connections.remote_connection import RemoteConnection
from .connections.k8s_connection import K8sConnection
from .connections.ssh_connection import SSHConnection

class TestEnv:
    def __init__(self,
                 namespace: str,
                 client_conns: list[RemoteConnection],
                 cta_cli_conns: list[RemoteConnection],
                 cta_frontend_conns: list[RemoteConnection],
                 cta_rmcd_conns: list[RemoteConnection],
                 cta_taped_conns: list[RemoteConnection],
                 eos_mgm_conns: list[RemoteConnection],
                 ):
        self.client = [ClientHost(conn) for conn in client_conns]
        self.ctacli = [CtaCliHost(conn) for conn in cta_cli_conns]
        self.ctafrontend = [CtaFrontendHost(conn) for conn in cta_frontend_conns]
        self.ctarmcd = [CtaRmcdHost(conn) for conn in cta_rmcd_conns]
        self.ctataped = [CtaTapedHost(conn) for conn in cta_taped_conns]
        self.eosmgm = [EosMgmHost(conn) for conn in eos_mgm_conns]
         # This is necessary until we have migrated all the current scripts
         # Of course it doesn't make sense to have a single namespace when in theory pods can reside in different ones
        self.namespace = namespace
        # TODO: this should eventually be defined elsewhere
        self.disk_instance_name="ctaeos"
        self.eos_base_dir="/eos/ctaeos"
        self.eos_preprod_dir=f"{self.eos_base_dir}/preprod"

    @staticmethod
    def fromNamespace(namespace: str):
        # Hardcoded for now, at some point we can make this nicely discover how many replicas there are of each
        return TestEnv(
            namespace=namespace,
            client_conns=[K8sConnection(namespace, "client-0", "client")],
            cta_cli_conns=[K8sConnection(namespace, "cta-cli-0", "cta-cli")],
            cta_frontend_conns=[K8sConnection(namespace, "cta-frontend-0", "cta-frontend")],
            cta_rmcd_conns=[K8sConnection(namespace, "cta-tpsrv01-0", "rmcd"),
                            K8sConnection(namespace, "cta-tpsrv02-0", "rmcd")],
            cta_taped_conns=[K8sConnection(namespace, "cta-tpsrv01-0", "taped-0"),
                             K8sConnection(namespace, "cta-tpsrv02-0", "taped-0")],
            eos_mgm_conns=[K8sConnection(namespace, "eos-mgm-0", "eos-mgm")],
        )

    @staticmethod
    def fromConfig(path: str):
        # Expects a path to a yaml file containing for each host how to connect
        # E.g.
        # client:
        #   - k8s:
        #       namespace: dev
        #       pod: client-0
        #       container: client
        # ctafrontend:
        #   - ssh:
        #       user: root
        #       host: ctapreproductionfrontend
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
                    connections.append(K8sConnection(namespace=k8s["namespace"], pod=k8s["pod"], container=k8s["container"]))
                elif "ssh" in connection:
                    ssh = connection["ssh"]
                    connections.append(SSHConnection(user=ssh["user"], host=ssh["host"]))
                else:
                    raise ValueError("Invalid connection configuration: must specify either 'k8s' or 'ssh'")

            return connections

        return TestEnv(
            namespace="not-supported",
            client_conn=create_connections(config, "client"),
            cta_cli_conn=create_connections(config, "ctacli"),
            cta_frontend_conn=create_connections(config, "ctafrontend"),
            cta_rmcd_conn=create_connections(config, "ctarmcd"),
            cta_taped_conn=create_connections(config, "ctataped"),
            eos_mgm_conn=create_connections(config, "eosmgm"),
        )

    # Mostly a convenience function that is arguably not very clean, but that is for later
    def execLocal(self, command: str, capture_output = False, throw_on_failure = True):
        full_command = f"bash -c \"{command}\""
        result = subprocess.run(full_command, shell=True, capture_output=capture_output)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f"local exec of {full_command} failed with exit code {result.returncode}: {result.stderr}")
        return result
