import subprocess
from typing import Optional

from .remote_connection import RemoteConnection

class K8sConnection(RemoteConnection):

    def __init__(self, namespace: str, pod: str, container: str):
        super().__init__()
        self.namespace = namespace
        self.pod = pod
        self.container = container

    def exec(self, command: str, capture_output = False, throw_on_failure = True) -> subprocess.CompletedProcess[bytes]:
        full_command = f"kubectl exec -n {self.namespace} {self.pod} -c {self.container} -- bash -c \"{command}\""
        result = subprocess.run(full_command, shell=True, capture_output=capture_output)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f"kubectl exec of {command} failed with exit code {result.returncode}: {result.stderr}")
        return result

    def copyTo(self, src_path: str, dst_path: str, throw_on_failure = True, permissions: Optional[str] = None) -> None:
        full_command = f"kubectl cp {src_path} {self.namespace}/{self.pod}:{dst_path} -c {self.container}"
        result = subprocess.run(full_command, shell=True)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f"kubectl cp failed with exit code {result.returncode}: {result.stderr}")
        if permissions:
            self.exec(f"chmod {permissions} {dst_path}")

    def copyFrom(self, src_path: str, dst_path: str, throw_on_failure = True) -> None:
        full_command = f"kubectl cp {self.namespace}/{self.pod}:{src_path} {dst_path} -c {self.container}"
        result = subprocess.run(full_command, shell=True)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f"kubectl cp failed with exit code {result.returncode}: {result.stderr}")
