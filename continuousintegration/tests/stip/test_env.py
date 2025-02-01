import yaml
from typing import Any

from hosts.client_host import ClientHost
from hosts.cta_cli_host import CtaCliHost
from hosts.cta_frontend_host import CtaFrontendHost
from hosts.cta_rmcd_host import CtaRmcdHost
from hosts.cta_taped_host import CtaTapedHost
from hosts.eos_mgm_host import EosMgmHost
from connections.remote_connection import RemoteConnection
from connections.k8s_connection import K8sConnection
from connections.ssh_connection import SSHConnection


# TODO: think about how to deal multiple replicas cleanly
# Right now this doesn't extend nicely in case tests need different replicas
# Maybe a dict and a sensible default?
class TestEnv:
    def __init__(self, client_conn: RemoteConnection,
                        cta_cli_conn: RemoteConnection,
                        cta_frontend_conn: RemoteConnection,
                        cta_rmcd_conn: RemoteConnection,
                        cta_taped_conn: RemoteConnection,
                        eos_mgm_conn: RemoteConnection
                        ):
        self.client = ClientHost(client_conn)
        self.ctacli = CtaCliHost(cta_cli_conn)
        self.ctafrontend = CtaFrontendHost(cta_frontend_conn)
        self.ctarmcd = CtaRmcdHost(cta_rmcd_conn)
        self.ctataped = CtaTapedHost(cta_taped_conn)
        self.eosmgm = EosMgmHost(eos_mgm_conn)

    @staticmethod
    def fromNamespace(namespace: str):
        return TestEnv(
            client_conn=K8sConnection(namespace, "client-0", "client"),
            cta_cli_conn=K8sConnection(namespace, "cta-cli-0", "client"),
            cta_frontend_conn=K8sConnection(namespace, "cta-frontend-0", "client"),
            cta_rmcd_conn=K8sConnection(namespace, "cta-tpsrv01-0", "rmcd"),
            cta_taped_conn=K8sConnection(namespace, "cta-tpsrv01-0", "taped"),
            eos_mgm_conn=K8sConnection(namespace, "eos-mgm-0", "eos-mgm"),
        )

    @staticmethod
    def fromConfig(path: str):
        # Expects a path to a yaml file containing for each host how to connect
        # E.g.
        # client:
        #   k8s:
        #     namespace: dev
        #     pod: client-0
        #     container: client
        # ctafrontend:
        #   ssh:
        #     user: root
        #     host: ctapreproductionfrontend
        with open(path, "r") as f:
            config = yaml.safe_load(f)

        def create_connection(config: Any, host: str):
            if host not in config:
                raise ValueError(f"Invalid connection configuration: missing host {host}")
            host_config = config[host]

            """Helper function to create either SSH or K8s connection."""
            if "k8s" in host_config:
                k8s = host_config["k8s"]
                return K8sConnection(namespace=k8s["namespace"], pod=k8s["pod"], container=k8s["container"])
            elif "ssh" in host_config:
                ssh = host_config["ssh"]
                return SSHConnection(user=ssh["user"], host=ssh["name"])
            else:
                raise ValueError("Invalid connection configuration: must specify either 'k8s' or 'ssh'")

        return TestEnv(
            client_conn=create_connection(config, "client"),
            cta_cli_conn=create_connection(config, "ctacli"),
            cta_frontend_conn=create_connection(config, "ctafrontend"),
            cta_rmcd_conn=create_connection(config, "ctarmcd"),
            cta_taped_conn=create_connection(config, "ctataped"),
            eos_mgm_conn=create_connection(config, "eosmgm"),
        )
