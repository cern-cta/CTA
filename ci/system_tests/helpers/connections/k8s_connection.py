# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import subprocess
from typing import Optional
from functools import cached_property

from .remote_connection import RemoteConnection


class K8sConnection(RemoteConnection):

    def __init__(self, namespace: str, pod: str, container: str):
        super().__init__()
        self.namespace = namespace
        self.pod = pod
        self.container = container

    @cached_property
    def name(self) -> str:
        return f"{self.pod}-{self.container}"

    @cached_property
    def description(self) -> str:
        return f"Kubernetes pod {self.pod}, container {self.container} in namespace {self.namespace}"

    def exec(self, command: str, capture_output=False, throw_on_failure=True) -> subprocess.CompletedProcess[bytes]:
        cmd = f'kubectl exec -n {self.namespace} {self.pod} -c {self.container} -- bash -c "{command}"'
        # When not throwing on failure, suppress stderr to avoid kubectl noise
        if not throw_on_failure and not capture_output:
            result = subprocess.run(cmd, shell=True, stderr=subprocess.DEVNULL)
        else:
            result = subprocess.run(cmd, shell=True, capture_output=capture_output)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f'"{cmd}" failed with exit code {result.returncode}: {result.stderr}')
        return result

    def copyTo(self, src_path: str, dst_path: str, throw_on_failure=True, permissions: Optional[str] = None) -> None:
        cmd = f"kubectl cp {src_path} {self.namespace}/{self.pod}:{dst_path} -c {self.container}"
        result = subprocess.run(cmd, shell=True)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f'"{cmd}" failed with exit code {result.returncode}: {result.stderr}')
        if permissions:
            self.exec(f"chmod -R {permissions} {dst_path}")

    def copyFrom(self, src_path: str, dst_path: str, throw_on_failure=True) -> None:
        cmd = f"kubectl cp {self.namespace}/{self.pod}:{src_path} {dst_path} -c {self.container}"
        result = subprocess.run(cmd, shell=True)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f'"{cmd}" failed with exit code {result.returncode}: {result.stderr}\n')

    def restart(self, throw_on_failure=True) -> None:
        cmd = f"kubectl -n {self.namespace} delete pod {self.pod}"
        result = subprocess.run(cmd, shell=True)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f'"{cmd}" failed with exit code {result.returncode}: {result.stderr}\n')

    def is_up(self) -> bool:
        cmd = (
            f"kubectl get pod {self.pod} -n {self.namespace} "
            f"-o jsonpath='{{.status.conditions[?(@.type==\"Ready\")].status}}'"
        )

        result = subprocess.run(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=1,
        )

        if result.returncode != 0:
            return False

        status = result.stdout.decode("utf-8").strip()
        return status == "True"
