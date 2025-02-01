from remote_connection import RemoteConnection
import subprocess

class K8sConnection(RemoteConnection):

    def __init__(self, namespace: str, pod: str, container: str):
        super().__init__()
        self.namespace = namespace
        self.pod = pod
        self.container = container

    def run(self, command: str) -> bool:
        full_command = f"kubectl exec -n {self.namespace} {self.pod} -c {self.container} -- bash -c \"{command}\""
        result = subprocess.run(full_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.returncode == 0

    def copyTo(self, src_path: str, dst_path: str) -> bool:
        full_command = f"kubectl cp {src_path} {self.namespace}/{self.pod}:{dst_path} -c {self.container}"
        result = subprocess.run(full_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.returncode == 0

    def copyFrom(self, src_path: str, dst_path: str) -> bool:
        full_command = f"kubectl cp {self.namespace}/{self.pod}:{src_path} {dst_path} -c {self.container}"
        result = subprocess.run(full_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.returncode == 0
