# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import shlex
import time
import uuid

import pytest
from _pytest.fixtures import SubRequest

from system_tests.helpers.connections.k8s_connection import K8sConnection


class TestRuntimeDeployment:
    @pytest.mark.parametrize(
        ("daemon_fixture", "expected_config_files"),
        [
            pytest.param("cta_maintd", ("catalogue", "telemetry"), id="maintd"),
            pytest.param("cta_rmcd", (), id="rmcd"),
        ],
    )
    def test_runtime_directory_correctness(
        self,
        request: SubRequest,
        daemon_fixture: str,
        expected_config_files: tuple[str, ...],
        postgres_scheduler_enabled: bool,
    ) -> None:
        # Compare deployed inputs with their runtime copies and verify the generated service metadata
        daemon = request.getfixturevalue(daemon_fixture)

        def assert_remote_files_equal(deployed_path: str, runtime_path: str) -> None:
            daemon.exec(
                f'test "$(sha256sum < {shlex.quote(deployed_path)})" = "$(sha256sum < {shlex.quote(runtime_path)})"'
            )

        # Files present for every service
        daemon.exec(f"jq -e -r '.service == \"cta-{daemon.process_name}\"' /run/cta/version.json >/dev/null")
        assert_remote_files_equal(f"/etc/cta/cta-{daemon.process_name}.toml", "/run/cta/config.toml")
        assert_remote_files_equal("/usr/share/cta/cta-logging.schema.json", "/run/cta/cta-logging.schema.json")
        daemon.exec("jq -e '.title == \"CTA Logging Schema\"' /run/cta/cta-logging.schema.json >/dev/null")

        # Determine which files we should actually check, because not every service exposes all of them
        config_files = {
            "catalogue": ("/etc/cta/cta-catalogue.conf", "/run/cta/catalogue.config_file"),
            "scheduler": ("/etc/cta/cta-scheduler.conf", "/run/cta/scheduler.config_file"),
            "telemetry": ("/etc/cta/cta-otel.yaml", "/run/cta/telemetry.config_file"),
        }
        files_to_check = list(expected_config_files)
        if daemon_fixture == "cta_maintd" and postgres_scheduler_enabled:
            files_to_check.append("scheduler")

        # Now compare
        for config_name in files_to_check:
            deployed_path, runtime_path = config_files[config_name]
            assert_remote_files_equal(deployed_path, runtime_path)

    @pytest.mark.parametrize(
        "daemon_fixture",
        [
            pytest.param("cta_maintd", id="maintd"),
            pytest.param("cta_rmcd", id="rmcd"),
        ],
    )
    def test_reopens_logfile_on_sighup(self, request: SubRequest, daemon_fixture: str):
        daemon = request.getfixturevalue(daemon_fixture)
        connection = daemon.conn
        before = connection.container_snapshot() if isinstance(connection, K8sConnection) else None
        log_file = shlex.quote(str(daemon.log_file_path))
        pid_command = f"pgrep -u cta {daemon.process_name}"
        pid = daemon.exec_with_output(pid_command).strip()
        assert pid.isdecimal(), f"Expected one {daemon.process_name} process, got {pid!r}"

        rotated = shlex.quote(f"{daemon.log_file_path}.pytest.{uuid.uuid4().hex}")

        # Start with an empty replacement so an earlier reopen message cannot satisfy this check.
        daemon.exec(f"sudo mv {log_file} {rotated}")
        daemon.exec(f"sudo install -o cta -g tape -m 0644 /dev/null {log_file}")
        daemon.exec(f"kill -s HUP {pid}")

        # FileLogger writes this message after reopening; /proc FD access requires unavailable capabilities.
        deadline = time.monotonic() + 5
        while True:
            reopened = daemon.exec(f"grep -Fq 'Log file descriptor reopened' {log_file}", throw_on_failure=False)
            if reopened.success:
                break
            assert time.monotonic() < deadline, f"{daemon.process_name} did not reopen {log_file} after SIGHUP"
            time.sleep(0.1)

        # A restart must not masquerade as successful in-process log rotation.
        assert daemon.exec_with_output(pid_command).strip() == pid

        if before is not None:
            after = connection.container_snapshot()
            assert (after.pod_uid, after.container_id, after.restart_count) == (
                before.pod_uid,
                before.container_id,
                before.restart_count,
            )


class TestGracefulRestart:
    """Require clean shutdown and recovery, with no user-visible failures or lost work.

    Busy scenarios confirm active work before restart and require correct data and bounded completion time.
    Allow normal client retries and session draining.
    """

    @pytest.mark.parametrize("daemon_fixture", ["cta_maintd", "cta_rmcd"], ids=["maintd", "rmcd"])
    def test_sigterm_exits_cleanly_and_restarts(self, request: SubRequest, daemon_fixture: str) -> None:
        daemon = request.getfixturevalue(daemon_fixture)
        connection = daemon.conn
        if not isinstance(connection, K8sConnection):
            pytest.skip("Container lifecycle checks require Kubernetes")

        daemon.wait_for_host_up()
        before = connection.container_snapshot()
        assert before.container_id
        assert before.ready
        # The launcher must replace itself so Kubernetes signals the daemon directly.
        pid = daemon.exec_with_output(f"pgrep -u cta -x cta-{daemon.process_name}")
        assert pid == "1", f"Expected {daemon.process_name} as PID 1, got {pid!r}"

        try:
            # Detach the sender so the exec connection closes before container shutdown.
            daemon.exec("(sleep 1; kill -TERM 1) </dev/null >/dev/null 2>&1 &")
            after = connection.wait_for_container_restart(before)
            termination = after.last_termination
            assert termination is not None
            assert termination.container_id == before.container_id
            assert termination.exit_code == 0, f"Unclean SIGTERM exit: {termination}"
            assert not termination.signal, f"Daemon was terminated by a signal: {termination}"
            assert daemon.exec_with_output(f"pgrep -u cta -x cta-{daemon.process_name}") == "1"
            if daemon_fixture == "cta_rmcd":
                # Readiness alone does not prove the restarted media changer can serve requests.
                daemon.list_tapes_in_library()
        finally:
            # Recover a stuck service so later suites can still run after a failed assertion.
            if not connection.is_up():
                daemon.restart()

    @pytest.mark.skip(reason="unimplemented")
    @pytest.mark.parametrize("operation", ["archive", "retrieve"])
    def test_workflow_api_restart_during_submission(self, operation: str) -> None:
        """Restart one workflow API replica during submissions; verify all requests complete correctly."""

    @pytest.mark.skip(reason="unimplemented")
    @pytest.mark.parametrize("operation", ["archive", "retrieve"])
    def test_maintd_restart_during_reporting(self, operation: str) -> None:
        """Restart maintd during reporting; verify all reports reach users without stranded work."""

    @pytest.mark.skip(reason="unimplemented")
    @pytest.mark.parametrize("operation", ["archive", "retrieve"])
    def test_taped_restart_during_transfer(self, operation: str) -> None:
        """Restart taped during transfer; verify session draining, cartridge cleanup, and remaining work."""


class TestDependencyRecovery:
    """Allow expected outage errors; require no lost accepted work, correct data, and bounded recovery.

    Restore dependencies after each fault and reconcile request outcomes, including client retries.
    """

    @pytest.mark.skip(reason="unimplemented")
    @pytest.mark.parametrize("database", ["catalogue", "scheduler"])
    @pytest.mark.parametrize("operation", ["archive", "retrieve"])
    def test_database_restart_during_work(self, database: str, operation: str) -> None:
        """Restart the database during active work; verify reconnection and completion after recovery.

        The scheduler database case requires the PostgreSQL scheduler.
        """

    @pytest.mark.skip(reason="unimplemented")
    @pytest.mark.parametrize("database", ["catalogue", "scheduler"])
    @pytest.mark.parametrize("operation", ["archive", "retrieve"])
    def test_database_outage_during_work(self, database: str, operation: str) -> None:
        """Hold the database unavailable during active work, then restore it and verify work recovers.

        The scheduler database case requires the PostgreSQL scheduler.
        """

    @pytest.mark.skip(reason="unimplemented")
    @pytest.mark.parametrize("operation", ["archive", "retrieve"])
    def test_taped_recovers_after_rmcd_outage(self, operation: str) -> None:
        """Make rmcd unavailable during a mount attempt; restore it and verify safe drive and job recovery."""


class TestForcedTerminationDuringWork:
    """Allow expected errors and retries; require no lost accepted work, correct data, and bounded recovery.

    Send SIGKILL during confirmed activity and reconcile outcomes after restart, including ambiguous submissions.
    """

    @pytest.mark.skip(reason="unimplemented")
    @pytest.mark.parametrize("operation", ["archive", "retrieve"])
    def test_workflow_api_killed_during_submission(self, operation: str) -> None:
        """Kill one workflow API replica during submissions; verify retries do not lose or duplicate logical work."""

    @pytest.mark.skip(reason="unimplemented")
    @pytest.mark.parametrize("operation", ["archive", "retrieve"])
    def test_maintd_killed_during_reporting(self, operation: str) -> None:
        """Kill maintd during reporting; verify pending reports recover without incorrect user-visible outcomes."""

    @pytest.mark.skip(reason="unimplemented")
    @pytest.mark.parametrize("operation", ["archive", "retrieve"])
    def test_taped_killed_during_transfer(self, operation: str) -> None:
        """Kill taped during transfer; verify loaded-cartridge cleanup and abandoned-job recovery.

        Enable startup recovery and verify file contents and catalogue consistency afterward.
        """
