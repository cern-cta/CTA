# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import re
import shlex
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Optional

import pytest
from _pytest.fixtures import SubRequest

from system_tests.helpers.connections.k8s_connection import K8sConnection
from system_tests.helpers.hosts import CtaCliHost, CtaTapedHost, EosClientHost
from system_tests.helpers.restart_expectations import RestartExpectations

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

# The main goal of these tests are to ensure that CTA behaves correctly to different signals:
# - SIGHUP should refresh the log descriptor
# - SIGTERM should gracefully shutdown the service
# Additionally, in a distributed setup, restarts (ideally even when abruptly killed by SIGKILL) should be transparent.
# Here transparent means: "no user-visible failures, no lost work, correct data, and completion within a bounded time"


def _restart_daemon_with_sigterm(
    daemon: Any,
    program: str,
    restart_expectations: RestartExpectations,
    *,
    timeout_secs: int = 120,
    transfer_log_message: Optional[str] = None,
    start_transfers: Optional[Callable[[], None]] = None,
) -> list[dict[str, Any]]:
    """Send SIGTERM, verify one clean container restart, and return the new log records.

    For a busy restart, arm the log trigger before starting transfers so short tape sessions cannot be missed.
    """
    connection = daemon.conn
    if not isinstance(connection, K8sConnection):
        pytest.skip("Container lifecycle checks require Kubernetes")

    deadline = time.monotonic() + timeout_secs
    daemon.wait_for_host_up(wait_timeout_secs=timeout_secs)
    before = connection.container_snapshot()
    assert before.container_id
    assert before.ready

    def assert_daemon_is_pid_one() -> None:
        # /proc/1/exe requires ptrace access; taped also changes its process name.
        # Check argv[0] directly so a shell launcher cannot masquerade as the daemon.
        command_line = daemon.exec_with_output("cat /proc/1/cmdline")
        executable = command_line.split("\0", 1)[0]
        assert executable.rsplit("/", 1)[-1] == program, (
            f"Expected {program} as PID 1, got command line {command_line!r}"
        )

    # The launcher must replace itself so Kubernetes signals the daemon directly.
    assert_daemon_is_pid_one()

    # The log volume survives a container restart. Snapshot complete records before sending SIGTERM.
    log_file = shlex.quote(str(daemon.log_file_path))
    previous_records = daemon.exec_with_output(f"cat {log_file}").splitlines()

    signal_trigger_pid: Optional[str] = None
    try:
        with restart_expectations.expect_container_restarts(daemon):
            if transfer_log_message is None:
                # Detach the sender so the exec connection closes before container shutdown.
                daemon.exec("(sleep 1; kill -TERM 1) </dev/null >/dev/null 2>&1 &")
            else:
                assert start_transfers is not None
                # Observe new records locally: catalogue polling can miss a fast virtual-tape transfer.
                trigger = (
                    f"until tail -n +{len(previous_records) + 1} {log_file} | "
                    f"grep -Fq {shlex.quote(transfer_log_message)}; do sleep 0.02; done; kill -TERM 1"
                )
                signal_trigger_pid = daemon.exec_with_output(
                    f"timeout {timeout_secs} sh -c {shlex.quote(trigger)} </dev/null >/dev/null 2>&1 & echo $!"
                )
                assert signal_trigger_pid is not None
                assert signal_trigger_pid.isdecimal()
                start_transfers()
            after = connection.wait_for_container_restart(before, timeout_secs=max(1, int(deadline - time.monotonic())))

        # Keep outcome assertions outside the restart allowance so failures do not leave restart tracking stale.
        termination = after.last_termination
        # Kubernetes may omit termination metadata; always require the lifecycle logs below.
        if termination is not None:
            assert termination.container_id == before.container_id
            assert termination.exit_code == 0, f"Unclean SIGTERM exit: {termination}"
            assert not termination.signal, f"Daemon was terminated by a signal: {termination}"
        else:
            print(f"{program}: termination metadata unavailable; verifying shutdown through lifecycle logs")
        assert_daemon_is_pid_one()
        expected_events = [
            "program_stopping",
            "program_stopped",
            "program_starting",
            "program_started",
        ]
        # Readiness can precede application startup completion (notably for rmcd).
        while True:
            records = daemon.exec_with_output(f"cat {log_file}").splitlines()
            assert records[: len(previous_records)] == previous_records, "Logs changed during restart"
            new_records = [json.loads(line) for line in records[len(previous_records) :]]
            events = [record.get("event_name") for record in new_records]
            lifecycle_events = [event for event in events if event in expected_events]
            # Fail immediately on duplicates or out-of-order transitions; wait only for missing events.
            assert lifecycle_events == expected_events[: len(lifecycle_events)], lifecycle_events
            if lifecycle_events == expected_events:
                break
            assert time.monotonic() < deadline, (
                f"{program} did not complete its restart lifecycle: {lifecycle_events!r}"
            )
            time.sleep(0.5)

        # No record may appear between the old process's final log and the new process's first log.
        assert events.index("program_starting") == events.index("program_stopped") + 1

        if program == "cta-rmcd":
            # Readiness alone does not prove the restarted media changer can serve requests.
            daemon.list_tapes_in_library()
    finally:
        # Cancel a pending sender if startup failed; never signal a reused PID in the restarted container.
        if signal_trigger_pid is not None and connection.container_snapshot().container_id == before.container_id:
            daemon.exec(f"kill {signal_trigger_pid}", throw_on_failure=False)
        # Recover a stuck service so later suites can still run after a failed assertion.
        if not connection.is_up():
            daemon.restart()

    return new_records


def _assert_tape_session_drained(
    records: list[dict[str, Any]], transfer_log_message: str, submitted_file_count: int
) -> None:
    """Require an active tape session to finish after SIGTERM, before all submitted files are transferred."""
    signal_record_index = next(
        i for i, record in enumerate(records) if record.get("message") == "Signal reactor received SIGTERM"
    )
    assert any(record.get("message") == transfer_log_message for record in records[:signal_record_index])
    stopped_record_index = next(i for i, record in enumerate(records) if record.get("event_name") == "program_stopped")
    transferred_file_count = sum(
        record.get("message") == transfer_log_message for record in records[:stopped_record_index]
    )
    assert 0 < transferred_file_count < submitted_file_count, (
        f"Expected shutdown with queued work remaining; transferred {transferred_file_count}/{submitted_file_count}"
    )
    completed_sessions = [
        record
        for record in records[signal_record_index:stopped_record_index]
        if record.get("event_name") == "tape_session_finished"
    ]
    assert completed_sessions, "Active tape session did not finish before process exit"
    assert all(record["status"] == "success" and record["wasTapeMounted"] for record in completed_sessions), (
        completed_sessions
    )


@pytest.fixture
def isolated_taped(
    cta_taped: CtaTapedHost, cta_cli: CtaCliHost, restart_expectations: RestartExpectations
) -> Iterator[CtaTapedHost]:
    """Yield a drive requested Down with limited job fetching and buffering.

    Keep other drives requested Down for the test; restore configuration and desired drive states afterward.
    """
    connection = cta_taped.conn
    if not isinstance(connection, K8sConnection):
        pytest.skip("Busy taped lifecycle checks require Kubernetes")
    drive_name = cta_taped.drive_name.strip()
    configs = connection.core.list_namespaced_config_map(connection.namespace).items
    matches = [
        config
        for config in configs
        if config.data is not None
        and "cta-taped.toml" in config.data
        and tomllib.loads(config.data["cta-taped.toml"])["drive"]["name"] == drive_name
    ]
    assert len(matches) == 1, f"Expected one configuration for {drive_name}"
    config = matches[0]
    assert config.data is not None
    assert config.metadata is not None
    config_name = config.metadata.name
    assert config_name is not None
    original = config.data["cta-taped.toml"]
    modified = original
    # Small fetch batches alone do not bound acquired work.
    # Disk readers can keep refilling the tape worker's task queue.
    # Limit buffer memory as well so a finite workload leaves jobs in the scheduler at shutdown.
    settings = {
        "transfers": {"buffer_count": "8", "buffer_size_bytes": "1048576", "disk_io_threads": "2"},
        "transfers.archive": {"fetch_max_files": "2", "flush_max_files": "1"},
        "transfers.retrieve": {"fetch_max_files": "2"},
        # RAO may raise the fetch limit to the drive's supported batch size.
        "transfers.retrieve.rao": {"enabled": "false"},
        # Keep the remaining queue observable until the test explicitly requests Up again.
        "drive.lifecycle.startup": {"auto_up": "false"},
    }
    # Preserve the deployed TOML layout and require every override to match exactly once.
    for section, values in settings.items():
        for key, value in values.items():
            pattern = rf"(?ms)(^\s*\[{re.escape(section)}\][^[]*?^\s*{key}\s*=)[^\n]*"
            modified, count = re.subn(pattern, rf"\g<1> {value}", modified)
            assert count == 1, f"Missing or duplicate setting: {section}.{key}"
    tomllib.loads(modified)
    drives = json.loads(cta_cli.exec_with_output("cta-admin --json drive ls"))

    def apply_config(text: str) -> None:
        connection.core.patch_namespaced_config_map(
            config_name, connection.namespace, {"data": {"cta-taped.toml": text}}
        )
        # ConfigMap subPath mounts require pod replacement to load the new contents.
        with restart_expectations.expect_pod_replacement(cta_taped):
            cta_taped.restart()
        assert tomllib.loads(cta_taped.exec_with_output("cat /run/cta/config.toml")) == tomllib.loads(text)

    with ExitStack() as cleanup:
        # Callbacks run in reverse order: stop work, restore configuration, then restore desired drive states.
        for drive in drives:
            if drive["desiredDriveState"] == "UP":
                cleanup.callback(cta_cli.set_drive_up, drive["driveName"], wait=False)
        cleanup.callback(apply_config, original)
        cleanup.callback(cta_cli.set_all_drives_down)
        cta_cli.set_all_drives_down()
        apply_config(modified)
        yield cta_taped


class TestRuntimeDeployment:
    @pytest.mark.parametrize(
        ("daemon_fixture", "expected_config_files"),
        [
            pytest.param("cta_maintd", ("catalogue", "scheduler", "telemetry"), id="maintd"),
            pytest.param("cta_taped", ("catalogue", "scheduler", "telemetry"), id="taped"),
            pytest.param("cta_rmcd", (), id="rmcd"),
        ],
    )
    def test_runtime_directory_correctness(
        self,
        request: SubRequest,
        daemon_fixture: str,
        expected_config_files: tuple[str, ...],
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

        # Now compare
        for config_name in files_to_check:
            deployed_path, runtime_path = config_files[config_name]
            assert_remote_files_equal(deployed_path, runtime_path)

    @pytest.mark.parametrize(
        "daemon_fixture",
        [
            pytest.param("cta_maintd", id="maintd"),
            pytest.param("cta_taped", id="taped"),
            pytest.param("cta_rmcd", id="rmcd"),
        ],
    )
    def test_sighup_reopens_logfile(self, request: SubRequest, daemon_fixture: str):
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

    @pytest.mark.parametrize(
        ("daemon_fixture", "program"),
        [
            ("cta_maintd", "cta-maintd"),
            ("cta_taped", "cta-taped"),
            ("cta_rmcd", "cta-rmcd"),
            ("cta_admin_api", "cta-admin-api"),
            ("cta_workflow_api", "cta-workflow-api"),
        ],
    )
    def test_sigterm_exits_cleanly_and_restarts(
        self, request: SubRequest, daemon_fixture: str, program: str, restart_expectations: RestartExpectations
    ) -> None:
        _restart_daemon_with_sigterm(request.getfixturevalue(daemon_fixture), program, restart_expectations)

    @pytest.mark.skip(reason="unimplemented")
    @pytest.mark.parametrize("operation", ["archive", "retrieve"])
    def test_workflow_api_restart_during_submission(self, operation: str) -> None:
        """Restart one workflow API replica during submissions; verify all requests complete correctly."""

    @pytest.mark.skip(reason="unimplemented")
    @pytest.mark.parametrize("operation", ["archive", "retrieve"])
    def test_maintd_restart_during_reporting(self, operation: str) -> None:
        """Restart maintd during reporting; verify all reports reach users without stranded work."""

    @pytest.mark.parametrize("operation", ["archive", "retrieve"])
    def test_taped_restart_during_transfer(
        self,
        operation: str,
        isolated_taped: CtaTapedHost,
        cta_cli: CtaCliHost,
        eos_client: EosClientHost,
        disk_instance_name: str,
        test_dir: Path,
        restart_expectations: RestartExpectations,
    ) -> None:
        """Stop during transfer, leave a scheduler backlog, then recover every file intact."""
        drive_name = isolated_taped.drive_name.strip()
        directory = test_dir / f"sigterm_{operation}_{uuid.uuid4().hex}"
        files = [directory / f"file_{i}" for i in range(64)]
        local_dir = eos_client.exec_with_output("mktemp -d /tmp/cta-lifecycle.XXXXXX")
        source = f"{local_dir}/source"
        deadline = time.monotonic() + 600

        def remaining_secs() -> int:
            seconds = int(deadline - time.monotonic())
            assert seconds > 0, f"{operation} restart workload exceeded its 600s deadline"
            return seconds

        def queued_file_count() -> int:
            queues = json.loads(cta_cli.exec_with_output("cta-admin --json showqueues"))
            mount_type = "ARCHIVE_FOR_USER" if operation == "archive" else "RETRIEVE"
            return sum(int(q["queuedFiles"]) for q in queues if q["mountType"] == mount_type)

        try:
            assert queued_file_count() == 0, "Busy restart test requires an empty queue for this operation"
            # A finite 1 GiB workload, with only a few jobs admitted at a time, avoids an unbounded producer.
            eos_client.exec(f"timeout 30 dd if=/dev/urandom of={shlex.quote(source)} bs=1M count=16 status=none")
            checksum = eos_client.exec_with_output(f"sha256sum {shlex.quote(source)}").split()[0]
            eos_client.exec(f"eos root://{disk_instance_name} mkdir -p {shlex.quote(str(directory))}")
            upload_command = (
                f"KRB5CCNAME=/tmp/user1/krb5cc_0 xrdcp {shlex.quote(source)} "
                f'{shlex.quote(f"root://{disk_instance_name}/{directory}/file_")}"$1"'
            )
            eos_client.exec(
                f"timeout {remaining_secs()} bash -o pipefail -c "
                + shlex.quote(f"seq 0 {len(files) - 1} | xargs -P 4 -n 1 bash -c {shlex.quote(upload_command)} _")
            )
            if operation == "retrieve":
                cta_cli.set_drive_up(drive_name, wait=False)
                eos_client.wait_for_directory_eviction(
                    disk_instance_name, directory, wait_timeout_secs=remaining_secs()
                )
                cta_cli.set_drive_down(drive_name)
                eos_client.retrieve_files(disk_instance_name, files, wait=False)

            # Wait for asynchronous EOS workflows to enqueue the full backlog before allowing a mount.
            enqueue_deadline = time.monotonic() + min(60, remaining_secs())
            while queued_file_count() < len(files):
                assert time.monotonic() < enqueue_deadline, "Not all files reached the scheduler queue"
                time.sleep(0.1)
            assert queued_file_count() == len(files), "Unexpected work appeared in the scheduler queue"
            transfer_log_message = (
                "File successfully transmitted to drive"
                if operation == "archive"
                else "File successfully read from tape"
            )
            records = _restart_daemon_with_sigterm(
                isolated_taped,
                "cta-taped",
                restart_expectations,
                timeout_secs=min(120, remaining_secs()),
                transfer_log_message=transfer_log_message,
                start_transfers=lambda: cta_cli.set_drive_up(drive_name, wait=False),
            )
            _assert_tape_session_drained(records, transfer_log_message, submitted_file_count=len(files))
            assert queued_file_count() > 0, "SIGTERM drained the entire scheduler queue instead of stopping fetching"
            # SIGTERM requests Down; explicitly authorize the restarted daemon to finish the backlog.
            cta_cli.set_drive_up(drive_name, wait=False)
            if operation == "archive":
                eos_client.wait_for_directory_eviction(
                    disk_instance_name, directory, wait_timeout_secs=remaining_secs()
                )
                eos_client.retrieve_files(disk_instance_name, files, wait=False)
            for path in files:
                eos_client.wait_for_file_retrieval(disk_instance_name, path, wait_timeout_secs=remaining_secs())

            # Read actual retrieved bytes, rather than trusting only catalogue or EOS residency metadata.
            download = f"{local_dir}/download"
            paths = " ".join(shlex.quote(str(path)) for path in files)
            verify_command = (
                "set -e; export KRB5CCNAME=/tmp/poweruser1/krb5cc_0; "
                f"for path in {paths}; do "
                f'xrdcp -f "root://{disk_instance_name}/$path" {shlex.quote(download)}; '
                f"printf '%s\\n' {shlex.quote(f'{checksum}  {download}')} | sha256sum --check; done"
            )
            eos_client.exec(f"timeout {remaining_secs()} bash -c {shlex.quote(verify_command)}")
        finally:
            eos_client.exec(f"rm -rf -- {shlex.quote(local_dir)}")


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
