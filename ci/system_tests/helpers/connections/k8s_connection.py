# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import os
import subprocess
import time
from pathlib import Path
from typing import Optional, Union, cast

from kubernetes import client, config
from kubernetes.client import ApiException, V1ObjectMeta, V1Pod
from kubernetes.stream import stream
from typing_extensions import override

from ..utils.timeout import Timeout
from .remote_connection import ExecResult, RemoteConnection


class K8sConnection(RemoteConnection):
    def __init__(self, namespace: str, label_selector: str, container: str, ordinal: int) -> None:
        assert len(container) > 1
        super().__init__()
        self.namespace = namespace
        self.label_selector = label_selector
        self.ordinal = ordinal
        self.container = container
        self._cached_pod = None
        config.load_kube_config()
        self.core = client.CoreV1Api()

    @property
    @override
    def name(self) -> str:
        return f"{self._pod_metadata(self._pod).name}-{self.container}"

    @property
    @override
    def description(self) -> str:
        return (
            f"Kubernetes pod {self._pod_metadata(self._pod).name}, "
            f"container {self.container} in namespace {self.namespace}"
        )

    @property
    def _pod(self) -> V1Pod:
        if self._cached_pod is None:
            self._cached_pod = self._resolve_pod()
        return self._cached_pod

    @staticmethod
    def _pod_metadata(pod: V1Pod) -> V1ObjectMeta:
        if pod.metadata is None:
            raise RuntimeError("Kubernetes pod metadata is unavailable")
        return pod.metadata

    @classmethod
    def _pod_name(cls, pod: V1Pod) -> str:
        name = cast(Optional[str], cls._pod_metadata(pod).name)
        if not name:
            raise RuntimeError("Kubernetes pod name is unavailable")
        return name

    @override
    def exec(
        self, command: str, capture_output: bool = False, throw_on_failure: bool = True, print_command: bool = False
    ) -> ExecResult:
        if print_command:
            print(command)
        full_command = ["/bin/sh", "-c", command]

        resp = stream(
            self.core.connect_get_namespaced_pod_exec,
            self._pod_metadata(self._pod).name,
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

    @override
    def copy_to(
        self,
        src_path: Union[str, Path],
        dst_path: Union[str, Path],
        throw_on_failure: bool = True,
        permissions: Optional[str] = None,
    ) -> None:
        # TODO: replace these kubectl calls so that we rely only on the SDK
        pod_target = f"{self.namespace}/{self._pod_metadata(self._pod).name}:{dst_path}"
        cmd = f"kubectl cp {src_path} {pod_target} -c {self.container}"
        result = subprocess.run(cmd, shell=True)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f'"{cmd}" failed with exit code {result.returncode}: {result.stderr}')
        if permissions:
            target = dst_path
            if os.fspath(dst_path).endswith("/"):
                target = os.path.join(dst_path, os.path.basename(src_path))
            self.exec(f"chmod {permissions} {target}")

    @override
    def copy_from(self, src_path: Union[str, Path], dst_path: Union[str, Path], throw_on_failure: bool = True) -> None:
        pod_source = f"{self.namespace}/{self._pod_metadata(self._pod).name}:{src_path}"
        cmd = f"kubectl cp {pod_source} {dst_path} -c {self.container}"
        result = subprocess.run(cmd, shell=True)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f'"{cmd}" failed with exit code {result.returncode}: {result.stderr}\n')

    @override
    def restart(self, throw_on_failure: bool = True) -> None:
        self._cached_pod = None  # Force resolve the pod before we restart
        metadata = self._pod_metadata(self._pod)
        uid = metadata.uid
        name = metadata.name
        try:
            self.core.delete_namespaced_pod(
                name=name,
                namespace=self.namespace,
            )
            # Wait until the pod no longer exists to ensure a restart has been triggered
            # Otherwise if we immediately start waiting for it, said wait might succeed because the process hasn't
            # terminated yet
            max_pod_disappear_secs = 60  # 1 minute should be more than enough for the pod to be deleted
            with Timeout(max_pod_disappear_secs) as t:
                while True:
                    pods = self.core.list_namespaced_pod(
                        namespace=self.namespace,
                        label_selector=self.label_selector,
                    ).items

                    if all(p.metadata.uid != uid for p in pods):
                        break

                    time.sleep(0.5)
                if t.expired:
                    raise TimeoutError(f"Failed to delete pod within timeout of {max_pod_disappear_secs} seconds")
            self._cached_pod = (
                None  # We may get a different pod (name) after restart, so ensure that we resolve it again
            )
        except ApiException as e:
            if throw_on_failure:
                raise RuntimeError(f"Pod deletion failed: {e}") from e

    @override
    def is_up(self) -> bool:
        try:
            pod = self._resolve_pod()
        except (ApiException, RuntimeError):
            return False

        if pod.status is None:
            return False

        conditions = pod.status.conditions or []
        for cond in conditions:
            if cond.type == "Ready":
                return cond.status == "True"

        return False

    @override
    def get_ip(self) -> str:
        try:
            pod = cast(
                V1Pod,
                self.core.read_namespaced_pod(
                    name=self._pod_metadata(self._pod).name,
                    namespace=self.namespace,
                ),
            )
        except ApiException as e:
            raise RuntimeError(f"Failed to get pod IP: {e}") from e

        if pod.status is None:
            raise RuntimeError("Pod IP not available")

        ip = pod.status.pod_ip
        if not ip:
            raise RuntimeError("Pod IP not available")

        return ip

    def _resolve_pod(self) -> V1Pod:
        pods = cast(
            list[V1Pod],
            self.core.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=self.label_selector,
            ).items,
        )

        pods.sort(key=self._pod_name)

        pods = [
            pod
            for pod in pods
            if pod.spec is not None and any(c.name == self.container for c in pod.spec.containers or [])
        ]

        if self.ordinal >= len(pods):
            raise RuntimeError(
                f'Expected at least {self.ordinal + 1} pod(s) matching "{self.label_selector}", found {len(pods)}'
            )

        return pods[self.ordinal]
