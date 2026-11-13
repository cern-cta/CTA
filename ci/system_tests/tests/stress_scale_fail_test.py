# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Failure-scenario companion to stress_test.py.

Runs the same archive/retrieve workload at the same scale (see presets/stress-scale-fail-values.yaml
and the stress-scale-fail-test CI job), but deliberately engineers three independent failure
classes into the run instead of avoiding them:

  1. Tape-boundary overflow (--stress-overflow-fraction, set on create_instance.sh, not in this
     file): retry-NEUTRAL, see ArchiveJobQueueRow::requeueJobBatch().
  2. VtlDrive's one-time transfer/retrieve fault injection (--stress-transfer-failure-rate /
     --stress-retrieve-failure-rate): consumes exactly one of a file's limited retries, never more
     — see taped/drive/VtlDrive.hpp's MediaTypeInfo::transferFailureRate and
     maybeInjectFault()'s one-time atomic marker.
  3. Deliberate mid-run pod kills, done HERE (test_disrupt_archive_mount and
     test_disrupt_report_batch below): one taped pod killed mid-mount, one
     diskReportArchive/diskReportRetrieve pod killed mid-batch, to prove maintd's cleanup routines
     (Archive/RetrieveInactiveMountActiveQueueRoutine, ResubmitInactiveReportingRoutine) actually
     recover that work rather than relying on a graceful shutdown path that a real failure would
     not take.

The test's point is that despite all of the above, every file still ends up archived and
retrieved, and no row is left behind in any *_FAILED_QUEUE or stuck IS_REPORTING. See
test_verify_no_jobs_left_behind, the final assertion.

This file deliberately mirrors stress_test.py's StressParams/stress_params fixture and the
archive/retrieve mechanics as closely as possible (same CLI options, same config keys) rather
than importing from it: there is no existing cross-test-module import convention in this
directory, and keeping this file self-contained avoids pytest collecting the same test functions
twice under two different module names.
"""

import asyncio
import contextlib
import json
import math
import random
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from system_tests.helpers.hosts import CtaCliHost, EosClientHost, EosMgmHost, SchedulerPostgresHost
from system_tests.helpers.test_config import TestConfig
from system_tests.helpers.test_env import TestEnv
from system_tests.helpers.utils.timeout import Timeout


@dataclass
class PrequeueParams:
    enabled: bool
    num_files_to_put_drives_up: int
    timeout_to_put_drives_up_sec: int
    check_interval_sec: int


@dataclass
class StressParams:
    total_files: int
    num_files_per_dir: int
    file_size: int
    io_threads: int
    batch_size: int
    check_copy_interval_sec: int
    write_files_in_chunks: bool
    check_archive_interval_sec: int
    max_no_progress_intervals: int
    max_acceptable_loss_percent: float
    prequeue: PrequeueParams
    retrieve_prequeue: PrequeueParams
    num_dirs: int = field(init=False)

    def __post_init__(self) -> None:
        if self.num_files_per_dir > 100000:
            raise ValueError(f"num_files_per_dir too high: {self.num_files_per_dir}, max allowed value is 100000")
        self.num_dirs = math.ceil(self.total_files / self.num_files_per_dir)
        if self.num_dirs > 50000:
            raise ValueError(f"num_dirs derived as {self.num_dirs} exceeds the max allowed value of 50000")


@pytest.fixture(scope="module")
def stress_params(test_config: TestConfig, request: pytest.FixtureRequest) -> StressParams:
    # Same config keys as stress_test.py's stress_params fixture — this job reads the SAME
    # test_pgsched_params.toml (see the CI job), just with the failure-injection CLI flags added
    # on top via create_instance.sh, which this fixture does not need to know about at all.
    stress_config = test_config["tests"]["stress"]
    prequeue_config = stress_config["prequeue"]

    toml_total = stress_config["total_files"]
    cli_total = request.config.getoption("--stress-total-files")
    total_files = int(cli_total) if cli_total is not None else toml_total

    cli_prequeue_enabled = request.config.getoption("--stress-prequeue-enabled")
    prequeue_enabled = cli_prequeue_enabled if cli_prequeue_enabled is not None else prequeue_config["enabled"]

    cli_prequeue_fraction = request.config.getoption("--stress-prequeue-fraction")
    prequeue_fraction = (
        cli_prequeue_fraction if cli_prequeue_fraction is not None else prequeue_config["prequeue_fraction"]
    )

    eos_queue_rate_hz = prequeue_config["eos_queue_rate_hz"]
    timeout_margin_factor = prequeue_config["timeout_margin_factor"]
    num_files_to_put_drives_up = int(total_files * prequeue_fraction)
    timeout_to_put_drives_up_sec = int(num_files_to_put_drives_up / eos_queue_rate_hz * timeout_margin_factor)

    cli_retrieve_prequeue_fraction = request.config.getoption("--stress-retrieve-prequeue-fraction")
    retrieve_prequeue_fraction = (
        cli_retrieve_prequeue_fraction
        if cli_retrieve_prequeue_fraction is not None
        else prequeue_config["retrieve_prequeue_fraction"]
    )
    retrieve_num_files_to_put_drives_up = int(total_files * retrieve_prequeue_fraction)
    retrieve_timeout_to_put_drives_up_sec = int(
        retrieve_num_files_to_put_drives_up / eos_queue_rate_hz * timeout_margin_factor
    )

    return StressParams(
        total_files=total_files,
        num_files_per_dir=stress_config["num_files_per_dir"],
        file_size=stress_config["file_size"],
        io_threads=stress_config["io_threads"],
        batch_size=stress_config["batch_size"],
        check_copy_interval_sec=stress_config["check_copy_interval_sec"],
        write_files_in_chunks=stress_config["write_files_in_chunks"],
        check_archive_interval_sec=stress_config["check_archive_interval_sec"],
        max_no_progress_intervals=stress_config["max_no_progress_intervals"],
        max_acceptable_loss_percent=stress_config["max_acceptable_loss_percent"],
        prequeue=PrequeueParams(
            enabled=prequeue_enabled,
            num_files_to_put_drives_up=num_files_to_put_drives_up,
            timeout_to_put_drives_up_sec=timeout_to_put_drives_up_sec,
            check_interval_sec=prequeue_config["check_interval_sec"],
        ),
        retrieve_prequeue=PrequeueParams(
            enabled=prequeue_enabled,
            num_files_to_put_drives_up=retrieve_num_files_to_put_drives_up,
            timeout_to_put_drives_up_sec=retrieve_timeout_to_put_drives_up_sec,
            check_interval_sec=prequeue_config["check_interval_sec"],
        ),
    )


# =========================================================================
#  Tests
# =========================================================================


def test_update_setup_for_max_powerrrr(env: TestEnv, cta_cli: CtaCliHost, eos_mgm: EosMgmHost) -> None:
    num_drives: int = len(env.cta_taped)
    cta_cli.exec(f"cta-admin vo ch --vo vo --writemaxdrives {num_drives} --readmaxdrives {num_drives}")
    eos_mgm.exec("eos vid set membership 0 +sudo")


@pytest.mark.asyncio
async def test_generate_and_copy_files(
    cta_cli: CtaCliHost,
    eos_client: EosClientHost,
    eos_mgm: EosMgmHost,
    stress_params: StressParams,
    test_dir: Path,
    remote_scripts_dir: Path,
) -> None:
    # Mechanically identical to stress_test.py's test_generate_and_copy_files — see that file for
    # inline comments on the monitor's error-handling and the staggered drive-up rationale. The
    # only behavioural difference this run has versus stress_test.py's is in the CONFIG
    # (overflow-fraction/failure-rate CLI flags, set by the CI job, not by this function), not in
    # the test logic itself.
    eos_client.copy_to(remote_scripts_dir / "eos_client" / "xrootd_archive.py", Path("/tmp/xrootd_archive.py"))
    eos_client.copy_to(remote_scripts_dir / "eos_client" / "count_files.py", Path("/tmp/count_files.py"))
    mgm_ip = eos_mgm.get_ip()
    total_file_count = stress_params.num_files_per_dir * stress_params.num_dirs

    print("Using the following parameters:")
    print(f"\tNumber of directories: {stress_params.num_dirs}")
    print(f"\tFiles per directory: {stress_params.num_files_per_dir}")
    print(f"\tTotal files: {total_file_count}")
    print(f"\tFile size: {stress_params.file_size}")
    print(f"\tIO threads: {stress_params.io_threads}")
    print(f"\tBatch size: {stress_params.batch_size}")
    print(f"\tPrequeueing: {stress_params.prequeue.enabled}")
    print(f"\tArchive: files to put drives up: {stress_params.prequeue.num_files_to_put_drives_up}")
    print(f"\tArchive: timeout to put drives up: {stress_params.prequeue.timeout_to_put_drives_up_sec}")

    if stress_params.prequeue.enabled:
        cta_cli.set_all_drives_down()
    else:
        cta_cli.set_all_drives_up()

    for subdir in range(stress_params.num_dirs):
        eos_mgm.mkdir(test_dir / str(subdir))

    timer_start = time.time()
    archive_command = (
        f"python3 -u /tmp/xrootd_archive.py "
        f"--eos-host {mgm_ip} "
        f"--dest-dir {test_dir} "
        f"--num-files-per-dir {stress_params.num_files_per_dir} "
        f"--num-dirs {stress_params.num_dirs} "
        f"--num-procs {stress_params.io_threads} "
        f"--file-size {stress_params.file_size} "
        f"--batch-size {stress_params.batch_size}"
    )
    if stress_params.write_files_in_chunks:
        archive_command += " --write-files-in-chunks"
    archive_future = eos_client.exec_async(archive_command)
    print("Archive process started")

    drives_up = not stress_params.prequeue.enabled
    stop_monitoring = asyncio.Event()

    async def monitor_copy_and_put_drives_up() -> None:
        nonlocal drives_up
        _consecutive_failures = 0
        while not stop_monitoring.is_set():
            try:
                num_files_so_far = eos_client.count_files_in_namespace(
                    eos_host=mgm_ip,
                    dest_dir=test_dir,
                    num_dirs=stress_params.num_dirs,
                    count_procs=5,
                )
                _consecutive_failures = 0
            except Exception as e:
                _consecutive_failures += 1
                print(
                    f"\t[copy monitor] K8s API error counting files "
                    f"({type(e).__name__}; failure {_consecutive_failures}/5)",
                    flush=True,
                )
                if not drives_up and time.time() - timer_start > stress_params.prequeue.timeout_to_put_drives_up_sec:
                    print("\tTimeout reached — putting drives UP", flush=True)
                    await asyncio.to_thread(cta_cli.set_all_drives_up_staggered, wait=False)
                    drives_up = True
                if _consecutive_failures >= 5 and drives_up:
                    print(
                        "\t[copy monitor] Stopping file monitor: drives already up, K8s API unresponsive",
                        flush=True,
                    )
                    break
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop_monitoring.wait(), stress_params.check_copy_interval_sec)
                continue

            print(f"\t[copy monitor] {num_files_so_far}/{total_file_count} files created", flush=True)

            if not drives_up:
                if num_files_so_far >= stress_params.prequeue.num_files_to_put_drives_up:
                    print(
                        f"\tThreshold ({stress_params.prequeue.num_files_to_put_drives_up}) reached — putting "
                        f"drives UP",
                        flush=True,
                    )
                    await asyncio.to_thread(cta_cli.set_all_drives_up_staggered, wait=False)
                    drives_up = True
                elif time.time() - timer_start > stress_params.prequeue.timeout_to_put_drives_up_sec:
                    print("\tTimeout reached — putting drives UP", flush=True)
                    await asyncio.to_thread(cta_cli.set_all_drives_up_staggered, wait=False)
                    drives_up = True

            sleep_time = (
                stress_params.prequeue.check_interval_sec if not drives_up else stress_params.check_copy_interval_sec
            )
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop_monitoring.wait(), sleep_time)

    monitor_task = asyncio.create_task(monitor_copy_and_put_drives_up())
    try:
        exec_result = await archive_future
    finally:
        stop_monitoring.set()
        await monitor_task

    if stress_params.prequeue.enabled and not drives_up:
        print("\tDisregarding drive-up threshold as it is larger than total files — putting drives UP now")
        await asyncio.to_thread(cta_cli.set_all_drives_up_staggered, wait=False)

    timer_end = time.time()
    if exec_result.stdout:
        print(f"Archive script output:\n{exec_result.stdout}")
    if not exec_result.success:
        print("Archive process failed")
        print(exec_result.stderr)

    num_files_copied = eos_client.count_files_in_namespace(
        eos_host=mgm_ip,
        dest_dir=test_dir,
        num_dirs=stress_params.num_dirs,
        count_procs=5,
    )
    if num_files_copied != total_file_count:
        print(f"Some files failed to copy over, expected: {total_file_count} and actual: {num_files_copied}")
    else:
        print(f"All {total_file_count} files copied successfully to {test_dir}")

    duration_seconds = timer_end - timer_start
    print("Copy complete:")
    print(f"\tTotal Files: {total_file_count}")
    print(f"\tFiles/s:     {total_file_count / duration_seconds:.2f}")


def test_disrupt_archive_mount(env: TestEnv, cta_cli: CtaCliHost) -> None:
    """Kill one taped pod while its drive is mid-mount, to exercise dead-mount recovery.

    Picks whichever drive happens to be TRANSFERING at the moment this runs (archival is still
    in flight on most runs when this executes, since it is collected right after
    test_generate_and_copy_files and pytest's addopts has -x but not strict ordering removal —
    tests in a module run in file order). If no drive is transferring (a legitimately possible,
    if unlucky, timing outcome — not a bug in this test), the kill is skipped rather than forced
    onto an idle drive, which would prove nothing about mount recovery specifically.

    Restarting the pod (not just killing the taped process) is deliberate: it reproduces what the
    cleanup routines actually have to handle in production — the drive's own DRIVE_STATE row
    disappearing/going stale along with the process, not a clean shutdown that updates it first.
    """
    taped_hosts = {t.drive_name: t for t in env.cta_taped}
    drives = json.loads(cta_cli.exec_with_output("cta-admin --json drive ls"))
    transferring = [d["driveName"] for d in drives if d["driveStatus"] == "TRANSFERING"]
    if not transferring:
        pytest.skip("No drive was transferring at the moment this ran; nothing to disrupt")

    victim = random.choice(transferring)
    host = taped_hosts.get(victim)
    if host is None:
        pytest.skip(f"Drive {victim} has no matching taped host in this environment")

    print(f"Disrupting archive mount: killing taped pod for drive {victim}")
    host.restart(wait_for_restart=True)
    print(f"taped pod for drive {victim} restarted")


def test_disrupt_report_batch(env: TestEnv) -> None:
    """Kill one diskReportArchive (or diskReportRetrieve) maintd pod mid-batch.

    Leaves whatever rows it was in the middle of reporting flagged IS_REPORTING, which only
    ResubmitInactiveReportingRoutine (sharing userActiveQueueCleanup's ageForCollectionSecs, see
    presets/stress-scale-fail-values.yaml) can clear — the routine pod's own graceful shutdown
    path, which would clear the flag itself, never runs here.
    """
    report_hosts = [h for h in env.cta_maintd if "diskreport" in h.name.lower()]
    if not report_hosts:
        pytest.skip("No diskReportArchive/diskReportRetrieve maintd pods found in this environment")

    victim = random.choice(report_hosts)
    print(f"Disrupting report batch: killing maintd pod {victim.name}")
    victim.restart(wait_for_restart=True)
    print(f"maintd pod {victim.name} restarted")


def test_wait_for_archival(eos_mgm: EosMgmHost, stress_params: StressParams, test_dir: Path) -> None:
    num_missing_files, loss_percent = eos_mgm.wait_for_archival_in_directory(
        archive_dir_path=test_dir,
        check_archive_interval_sec=stress_params.check_archive_interval_sec,
        max_no_progress_intervals=stress_params.max_no_progress_intervals,
    )
    print(f"Missing files: {num_missing_files}")
    print(f"Loss: {loss_percent:.2f}%")

    # Unlike stress_test.py, this test does NOT tolerate max_acceptable_loss_percent: the whole
    # point of this run is that the three injected failure classes are all individually
    # recoverable (see the module docstring), so a genuine loss here means one of them exhausted
    # a file's retry budget or the cleanup routines failed to reclaim a dead mount/batch — a real
    # regression, not expected noise to tolerate.
    assert num_missing_files == 0, (
        f"{num_missing_files} files were not archived despite all injected failures being "
        f"individually recoverable — see the module docstring for why this should be impossible"
    )


def test_kinit_poweruser(eos_client: EosClientHost, krb5_realm: str) -> None:
    eos_client.exec("mkdir -p /tmp/poweruser1")
    eos_client.exec(f"KRB5CCNAME=/tmp/poweruser1/krb5cc_0 kinit -kt /root/poweruser1.keytab poweruser1@{krb5_realm}")


@pytest.mark.asyncio
async def test_request_files_for_retrieve(
    cta_cli: CtaCliHost,
    eos_client: EosClientHost,
    eos_mgm: EosMgmHost,
    stress_params: StressParams,
    test_dir: Path,
    remote_scripts_dir: Path,
) -> None:
    # Mechanically identical to stress_test.py's test_request_files_for_retrieve.
    eos_client.copy_to(remote_scripts_dir / "eos_client" / "xrootd_retrieve.py", Path("/tmp/xrootd_retrieve.py"))
    archive_directory = test_dir
    mgm_ip = eos_mgm.get_ip()

    print("Sleeping 60 seconds to allow MGM-FST communication to settle after disk copy deletion", flush=True)
    await asyncio.sleep(60)

    if stress_params.retrieve_prequeue.enabled:
        cta_cli.set_all_drives_down()
    else:
        cta_cli.set_all_drives_up()

    total_file_count = stress_params.num_dirs * stress_params.num_files_per_dir

    timer_start = time.time()
    retrieve_future = eos_client.retrieve_async(
        eos_host=mgm_ip,
        dest_dir=archive_directory,
        num_dirs=stress_params.num_dirs,
        num_procs=stress_params.io_threads,
        batch_size=stress_params.batch_size,
    )
    print("Retrieve process started")

    drives_up = not stress_params.retrieve_prequeue.enabled
    stop_monitoring = asyncio.Event()

    async def monitor_retrieve_and_put_drives_up() -> None:
        nonlocal drives_up
        while not stop_monitoring.is_set():
            queued = cta_cli.retrieve_queue_file_count()
            print(f"\t[retrieve monitor] {queued}/{total_file_count} retrieve requests queued in CTA", flush=True)

            if not drives_up:
                if queued >= stress_params.retrieve_prequeue.num_files_to_put_drives_up:
                    print(
                        f"\tThreshold ({stress_params.retrieve_prequeue.num_files_to_put_drives_up}) reached"
                        f" — putting drives UP",
                        flush=True,
                    )
                    await asyncio.to_thread(cta_cli.set_all_drives_up_staggered, wait=False)
                    drives_up = True
                elif time.time() - timer_start > stress_params.retrieve_prequeue.timeout_to_put_drives_up_sec:
                    print("\tTimeout reached — putting drives UP", flush=True)
                    await asyncio.to_thread(cta_cli.set_all_drives_up_staggered, wait=False)
                    drives_up = True

            sleep_time = (
                stress_params.retrieve_prequeue.check_interval_sec
                if not drives_up
                else stress_params.check_copy_interval_sec
            )
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop_monitoring.wait(), sleep_time)

    monitor_task = asyncio.create_task(monitor_retrieve_and_put_drives_up())
    try:
        exec_result = await retrieve_future
    finally:
        stop_monitoring.set()
        await monitor_task

    if stress_params.retrieve_prequeue.enabled and not drives_up:
        print("\tDisregarding drive-up threshold — putting drives UP now")
        await asyncio.to_thread(cta_cli.set_all_drives_up_staggered, wait=False)

    if exec_result.stdout:
        print(f"Retrieve script output:\n{exec_result.stdout}")
    if not exec_result.success:
        print("Retrieve process failed")
        print(exec_result.stderr)


def test_wait_for_retrieval(eos_mgm: EosMgmHost, stress_params: StressParams, test_dir: Path) -> None:
    num_missing, loss_percent = eos_mgm.wait_for_retrieval_in_directory(
        archive_dir_path=test_dir,
        check_retrieve_interval_sec=stress_params.check_archive_interval_sec,
        max_no_progress_intervals=stress_params.max_no_progress_intervals,
    )
    print(f"Missing retrievals: {num_missing}")
    print(f"Loss: {loss_percent:.2f}%")

    # Same zero-tolerance rationale as test_wait_for_archival above.
    assert num_missing == 0, (
        f"{num_missing} files were not retrieved despite all injected failures being individually "
        f"recoverable — see the module docstring for why this should be impossible"
    )


def test_verify_no_jobs_left_behind(scheduler_postgres: SchedulerPostgresHost) -> None:
    """The real proof this test exists for.

    Every queue the three failure classes can leave rows in must be empty by the time archival
    and retrieval both report zero missing files.

    - ARCHIVE_FAILED_QUEUE / RETRIEVE_FAILED_QUEUE empty: no file permanently exhausted its retry
      budget (see the module docstring for why that should be structurally impossible here).
    - No row anywhere still flagged IS_REPORTING: ResubmitInactiveReportingRoutine actually
      cleared whatever test_disrupt_report_batch left stuck, rather than it rotting forever.
    - ARCHIVE_PENDING_QUEUE / RETRIEVE_PENDING_QUEUE empty: nothing is still waiting to be picked
      up by a mount — test_wait_for_archival/test_wait_for_retrieval already reached zero missing,
      so this is a consistency check on top of that, not a separate wait.
    """
    checks = {
        "ARCHIVE_FAILED_QUEUE": "SELECT COUNT(*) FROM ARCHIVE_FAILED_QUEUE",
        "RETRIEVE_FAILED_QUEUE": "SELECT COUNT(*) FROM RETRIEVE_FAILED_QUEUE",
        "ARCHIVE_PENDING_QUEUE": "SELECT COUNT(*) FROM ARCHIVE_PENDING_QUEUE",
        "RETRIEVE_PENDING_QUEUE": "SELECT COUNT(*) FROM RETRIEVE_PENDING_QUEUE",
        "ARCHIVE_ACTIVE_QUEUE stuck IS_REPORTING": (
            "SELECT COUNT(*) FROM ARCHIVE_ACTIVE_QUEUE WHERE IS_REPORTING IS TRUE"
        ),
        "RETRIEVE_ACTIVE_QUEUE stuck IS_REPORTING": (
            "SELECT COUNT(*) FROM RETRIEVE_ACTIVE_QUEUE WHERE IS_REPORTING IS TRUE"
        ),
    }
    failures: list[str] = []
    for label, sql in checks.items():
        count = int(scheduler_postgres.run_sql(sql).strip())
        print(f"\t{label}: {count}")
        if count != 0:
            failures.append(f"{label} has {count} row(s), expected 0")

    assert not failures, (
        "Found leftover rows after the run — the cleanup routines did not fully recover from the "
        "injected failures:\n" + "\n".join(failures)
    )


def test_wait_for_dead_mount_recovery(scheduler_postgres: SchedulerPostgresHost) -> None:
    """A narrower, earlier check than test_verify_no_jobs_left_behind.

    Specifically waits for MOUNT_QUEUE_LAST_FETCH to stop naming the mount
    test_disrupt_archive_mount killed, within a bounded window derived from
    presets/stress-scale-fail-values.yaml's
    userActiveQueueCleanup.ageForCollectionSecs (900 s) plus its poll interval (60 s). Run before
    the final assertion so a timeout here points directly at
    ArchiveInactiveMountActiveQueueRoutine/RetrieveInactiveMountActiveQueueRoutine rather than
    being lumped into test_verify_no_jobs_left_behind's more general failure.
    """
    # 900 s age + 60 s poll + 60 s margin for the pass itself to run and commit.
    timeout_secs = 900 + 60 + 60
    sql = "SELECT COUNT(*) FROM MOUNT_QUEUE_LAST_FETCH"
    with Timeout(timeout_secs) as t:
        count = -1
        while not t.expired:
            count = int(scheduler_postgres.run_sql(sql).strip())
            if count == 0:
                print(f"MOUNT_QUEUE_LAST_FETCH drained (0 rows) within {timeout_secs}s")
                return
            time.sleep(10)
    pytest.fail(
        f"MOUNT_QUEUE_LAST_FETCH still has {count} row(s) after {timeout_secs}s — "
        f"the inactive-mount cleanup routine did not reclaim the disrupted mount in time"
    )
