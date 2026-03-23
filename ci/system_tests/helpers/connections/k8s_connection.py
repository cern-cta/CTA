# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import subprocess
from typing import Optional
from functools import cached_property

from .remote_connection import RemoteConnection, ExecResult

from kubernetes import client, config
from kubernetes.stream import stream
import time


class K8sConnection(RemoteConnection):

    def __init__(self, namespace: str, pod: str, container: str):
        super().__init__()
        self.namespace = namespace
        self.pod = pod
        self.container = container
        config.load_kube_config()
        self.core = client.CoreV1Api()

    @cached_property
    def name(self) -> str:
        return f"{self.pod}-{self.container}"

    @cached_property
    def description(self) -> str:
        return f"Kubernetes pod {self.pod}, container {self.container} in namespace {self.namespace}"

    def exec(self, command: str, capture_output=False, throw_on_failure=True) -> ExecResult:
        full_command = ["/bin/sh", "-c", command]

        resp = stream(
            self.core.connect_get_namespaced_pod_exec,
            self.pod,
            self.namespace,
            container=self.container,
            command=full_command,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=False,
        )

        stdout_chunks = []
        stderr_chunks = []

        while resp.is_open():
            resp.update(timeout=0.1)

            if resp.peek_stdout():
                chunk = resp.read_stdout()
                if not capture_output:
                    print(chunk, end="")
                else:
                    stdout_chunks.append(chunk)

            if resp.peek_stderr():
                chunk = resp.read_stderr()
                # In contrast to stdout, we always need stderr in case we have issues
                stderr_chunks.append(chunk)
                if not capture_output:
                    print(chunk, end="")

        resp.close()

        success = resp.returncode == 0
        if throw_on_failure and not success:
            raise RuntimeError(f'"{full_command}" failed with exit code {resp.returncode}: {"".join(stderr_chunks)}')

        if capture_output:
            stdout = "".join(stdout_chunks)
            stderr = "".join(stderr_chunks)
        else:
            stdout = ""
            stderr = ""

        return ExecResult(stdout=stdout, stderr=stderr, success=success)

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
        try:
            self.core.delete_namespaced_pod(
                name=self.pod,
                namespace=self.namespace,
            )
            # Wait until the pod is no longer ready to ensure a restart has been triggered
            while self.is_up():
                time.sleep(1)
        except client.exceptions.ApiException as e:
            if throw_on_failure:
                raise RuntimeError(f"Pod deletion failed: {e}")

    def is_up(self) -> bool:
        try:
            pod = self.core.read_namespaced_pod(
                name=self.pod,
                namespace=self.namespace,
            )
        except client.exceptions.ApiException:
            return False

        conditions = pod.status.conditions or []
        for cond in conditions:
            if cond.type == "Ready":
                return cond.status == "True"

        return False

    def get_ip(self) -> str:
        try:
            pod = self.core.read_namespaced_pod(
                name=self.pod,
                namespace=self.namespace,
            )
        except client.exceptions.ApiException as e:
            raise RuntimeError(f"Failed to get pod IP: {e}")

        ip = pod.status.pod_ip
        if not ip:
            raise RuntimeError("Pod IP not available")

        return ip
