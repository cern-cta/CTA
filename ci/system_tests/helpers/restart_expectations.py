# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

from collections.abc import Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass

from system_tests.helpers.connections.k8s_connection import K8sConnection
from system_tests.helpers.hosts.remote_host import RemoteHost


@dataclass(frozen=True)
class ReplicaGroup:
    namespace: str
    selector: str
    container: str


@dataclass(frozen=True)
class ContainerKey:
    namespace: str
    pod_uid: str
    container: str


@dataclass(frozen=True)
class ObservedContainer:
    group: ReplicaGroup
    pod_name: str
    container_id: str | None
    restart_count: int

    @property
    def description(self) -> str:
        return f"{self.group.namespace}/{self.pod_name}/{self.group.container}"


class RestartExpectations:
    """Track CTA containers across the suite, allowing only explicitly scoped lifecycle changes."""

    def __init__(self) -> None:
        self._connections: dict[ReplicaGroup, K8sConnection] = {}
        self._baseline: dict[ContainerKey, ObservedContainer] = {}

    def start(self, hosts: Sequence[RemoteHost]) -> None:
        for host in hosts:
            connection = host.conn
            if isinstance(connection, K8sConnection):
                group = ReplicaGroup(connection.namespace, connection.label_selector, connection.container)
                self._connections[group] = connection
        self._baseline = self._snapshot()

    def _snapshot(self) -> dict[ContainerKey, ObservedContainer]:
        containers = {}
        # Query entire replica groups so pod-name ordering cannot hide a replacement.
        for group, connection in self._connections.items():
            pods = connection.core.list_namespaced_pod(namespace=group.namespace, label_selector=group.selector).items
            for pod in pods:
                assert pod.metadata is not None, "Pod metadata unavailable"
                assert pod.status is not None, f"Pod status unavailable for {group.namespace}/{pod.metadata.name}"
                statuses = [s for s in pod.status.container_statuses or [] if s.name == group.container]
                assert statuses, (
                    f"Container status unavailable for {group.namespace}/{pod.metadata.name}/{group.container}"
                )
                status = statuses[0]
                key = ContainerKey(group.namespace, pod.metadata.uid, group.container)
                containers[key] = ObservedContainer(group, pod.metadata.name, status.container_id, status.restart_count)
        return containers

    def reset_after_teardown(self) -> None:
        """Establish a fresh baseline after deliberate suite teardown."""
        self._baseline = self._snapshot()

    def verify(self) -> None:
        """Fail on any unaccounted restart or pod replacement since the last successful check."""
        self._verify()

    def _verify(self, target: ContainerKey | None = None, count: int = 0, replace_pod: bool = False) -> None:
        current = self._snapshot()
        removed = self._baseline.keys() - current.keys()
        added = current.keys() - self._baseline.keys()
        errors = []
        removed_names = sorted(self._baseline[key].description for key in removed)
        added_names = sorted(current[key].description for key in added)
        changes = f"removed={removed_names}, added={added_names}"
        if not replace_pod:
            if removed or added:
                errors.append(f"Unexpected pod changes: {changes}")
        elif target is None:
            raise ValueError("A target container is required for an expected pod replacement")
        elif removed != {target} or len(added) != 1:
            errors.append(f"Expected replacement of {self._baseline[target].description}; {changes}")
        else:
            replacement = current[next(iter(added))]
            if replacement.group != self._baseline[target].group:
                errors.append(f"{replacement.description}: replacement belongs to a different replica group")
            if replacement.restart_count != 0:
                errors.append(
                    f"{replacement.description}: replacement already restarted {replacement.restart_count} times"
                )

        for key in self._baseline.keys() & current.keys():
            before, after = self._baseline[key], current[key]
            delta = after.restart_count - before.restart_count
            expected = count if key == target else 0
            if delta != expected:
                errors.append(f"{after.description}: expected {expected} restarts, observed {delta}")
            if delta == 0 and before.container_id != after.container_id:
                errors.append(
                    f"{after.description}: container identity changed without a restart count change: "
                    f"{before.container_id} -> {after.container_id}"
                )
        assert not errors, "Unexpected CTA lifecycle changes:\n" + "\n".join(errors)
        self._baseline = current

    @contextmanager
    def _expect(self, host: RemoteHost, *, count: int = 0, replace_pod: bool = False) -> Generator[None, None, None]:
        if not isinstance(host.conn, K8sConnection):
            yield
            return
        # Reject earlier failures before granting an allowance for this operation.
        self.verify()
        snapshot = host.conn.container_snapshot()
        key = ContainerKey(host.conn.namespace, snapshot.pod_uid, host.conn.container)
        assert key in self._baseline, f"Host is not tracked: {host.name}"
        yield
        self._verify(target=key, count=count, replace_pod=replace_pod)

    @contextmanager
    def expect_container_restarts(self, host: RemoteHost, count: int = 1) -> Generator[None, None, None]:
        """Allow exactly count container restarts during an operation, retaining the same pod."""
        assert count > 0, "Expected restart count must be positive"
        with self._expect(host, count=count):
            yield

    @contextmanager
    def expect_pod_replacement(self, host: RemoteHost) -> Generator[None, None, None]:
        """Allow one pod replacement, with no additional container restarts."""
        with self._expect(host, replace_pod=True):
            yield
