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
     test_disrupt_report_batch below): up to NUM_MOUNTS_TO_DISRUPT taped pods killed mid-mount,
     up to NUM_REPORT_BATCHES_TO_DISRUPT diskReportArchive/diskReportRetrieve pods killed
     mid-batch, to prove maintd's cleanup routines
     (Archive/RetrieveInactiveMountActiveQueueRoutine, ResubmitInactiveReportingRoutine) actually
     recover that work rather than relying on a graceful shutdown path that a real failure would
     not take.

The test's point is that despite all of the above, archival/retrieval loss stays within the same
max_acceptable_loss_percent tolerance as stress-scale-test, *_FAILED_QUEUE stays within that same
tolerance (a row there only exists for a file that actually exhausted its retry budget), and no
row anywhere is left stuck IS_REPORTING. See test_verify_no_jobs_left_behind, the final
assertion.

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

from system_tests.helpers.hosts import (
    CtaCliHost,
    CtaMaintdHost,
    CtaTapedHost,
    EosClientHost,
    EosMgmHost,
    SchedulerPostgresHost,
)
from system_tests.helpers.test_config import TestConfig
from system_tests.helpers.test_env import TestEnv
from system_tests.helpers.utils.timeout import Timeout

# Wall-clock bookmarks shared across test functions in this module (which run in file order within
# one process — see the module docstring). Used to pace the deliberate disruptions below: killing a
# mount or a report pod too early (before drives are actually transferring, or too soon after the
# previous disruption) exercises a degenerate case — nothing to kill yet, or stacking two
# disruptions' recovery windows on top of each other — rather than "one disruption, fully into
# steady-state processing, recovers cleanly."
_disruption_timing: dict[str, float] = {}

MIN_SECS_SINCE_DRIVES_UP = 600
MIN_SECS_SINCE_PREVIOUS_DISRUPTION = 600


def _sleep_until_min_gap(reference_key: str, min_gap_secs: int, what: str) -> None:
    """Block until at least min_gap_secs have passed since _disruption_timing[reference_key] was
    recorded. No-ops (with a warning) if that bookmark was never set — should not happen in a
    normal run, but a missing bookmark must never turn into disrupting immediately instead of not
    at all.
    """
    reference_time = _disruption_timing.get(reference_key)
    if reference_time is None:
        print(f"\t[timing] No '{reference_key}' bookmark recorded — proceeding with {what} immediately")
        return
    remaining = min_gap_secs - (time.time() - reference_time)
    if remaining > 0:
        print(f"\t[timing] Waiting {remaining:.0f}s more before {what} (min gap: {min_gap_secs}s since {reference_key})")
        time.sleep(remaining)


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
        _disruption_timing["archive_drives_up_time"] = time.time()

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
                    _disruption_timing["archive_drives_up_time"] = time.time()
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
                    _disruption_timing["archive_drives_up_time"] = time.time()
                elif time.time() - timer_start > stress_params.prequeue.timeout_to_put_drives_up_sec:
                    print("\tTimeout reached — putting drives UP", flush=True)
                    await asyncio.to_thread(cta_cli.set_all_drives_up_staggered, wait=False)
                    drives_up = True
                    _disruption_timing["archive_drives_up_time"] = time.time()

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
        _disruption_timing["archive_drives_up_time"] = time.time()

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


NUM_MOUNTS_TO_DISRUPT = 10
NUM_REPORT_BATCHES_TO_DISRUPT = 2


def test_disrupt_archive_mount(env: TestEnv, cta_cli: CtaCliHost) -> None:
    """Kill up to NUM_MOUNTS_TO_DISRUPT taped pods while their drives are mid-mount, to exercise
    dead-mount recovery under multiple simultaneous losses, not just one.

    Waits until at least MIN_SECS_SINCE_DRIVES_UP has passed since drives actually went up (see
    test_generate_and_copy_files) before looking for a drive to disrupt. Running this check right
    after test_generate_and_copy_files returns is NOT the same as "well into steady-state
    processing": drives may have only just been told to come up (set_all_drives_up_staggered
    spreads that over time) with mount-delay jitter (up to mountDelayMaxMs) still ahead of them,
    so without this wait the drive-status check below can easily find nothing TRANSFERING yet and
    skip — exercising "disruption during prequeueing/mount startup" instead of the intended
    "disruption of a drive that has been transferring for a while."

    Picks whichever drives happen to be TRANSFERING at the moment this runs. If fewer than
    NUM_MOUNTS_TO_DISRUPT drives are transferring (a legitimately possible, if unlucky, timing
    outcome — not a bug in this test), as many as are available get disrupted rather than forcing
    idle drives, which would prove nothing about mount recovery specifically.

    All victims are killed before waiting on any of them, so their pods go down together
    (simultaneous loss) rather than one at a time. Restarting the pod (not just killing the taped
    process) is deliberate: it reproduces what the cleanup routines actually have to handle in
    production — the drive's own DRIVE_STATE row disappearing/going stale along with the process,
    not a clean shutdown that updates it first.
    """
    _sleep_until_min_gap("archive_drives_up_time", MIN_SECS_SINCE_DRIVES_UP, "disrupting an archive mount")
    try:
        taped_hosts = {t.drive_name: t for t in env.cta_taped}
        drives = json.loads(cta_cli.exec_with_output("cta-admin --json drive ls"))
        transferring = [d["driveName"] for d in drives if d["driveStatus"] == "TRANSFERING"]
        if not transferring:
            pytest.skip("No drive was transferring at the moment this ran; nothing to disrupt")

        victims = random.sample(transferring, k=min(NUM_MOUNTS_TO_DISRUPT, len(transferring)))
        hosts: list[tuple[str, CtaTapedHost]] = []
        for victim in victims:
            host = taped_hosts.get(victim)
            if host is None:
                print(f"Drive {victim} has no matching taped host in this environment, skipping it")
                continue
            print(f"Disrupting archive mount: killing taped pod for drive {victim}")
            host.restart(wait_for_restart=False)
            hosts.append((victim, host))

        if not hosts:
            pytest.skip("None of the selected drives had a matching taped host in this environment")

        for victim, host in hosts:
            host.wait_for_host_up()
            print(f"taped pod for drive {victim} restarted")
    finally:
        # Recorded whether this disruption actually happened or was skipped (nothing transferring
        # yet, no matching host): either way, this is the reference point test_disrupt_report_batch
        # paces its own gap from — a skip here should not let the next disruption land immediately.
        _disruption_timing["archive_mount_disrupted_time"] = time.time()


def test_disrupt_report_batch(env: TestEnv) -> None:
    """Kill up to NUM_REPORT_BATCHES_TO_DISRUPT diskReportArchive/diskReportRetrieve maintd pods
    mid-batch, instead of just one, so more than one in-flight batch is left behind at once.

    Leaves whatever rows each was in the middle of reporting flagged IS_REPORTING, which only
    ResubmitInactiveReportingRoutine (sharing userActiveQueueCleanup's ageForCollectionSecs, see
    presets/stress-scale-fail-values.yaml) can clear — the routine pods' own graceful shutdown
    path, which would clear the flag itself, never runs here.

    Victims are spread round-robin across report TYPES (diskReportArchive, diskReportRetrieve)
    before picking randomly within a type: a flat random.sample() across both types combined has
    a real chance (3 replicas each, by default — C(3,2)/C(6,2) = 20% for k=2) of landing twice on
    the SAME type, and during the archive phase diskReportRetrieve replicas are still idle (no
    retrieve work queued yet), so that outcome silently wipes out 2 of only 3 real
    diskReportArchive replicas at once — a much bigger capacity hit than "2 different disk
    reporters" is meant to exercise, and enough to starve archive reporting for several minutes.

    Waits until at least MIN_SECS_SINCE_PREVIOUS_DISRUPTION has passed since
    test_disrupt_archive_mount ran (whether it actually disrupted a mount or found nothing to
    disrupt) before killing anything here: stacking this disruption's recovery window directly on
    top of the mount disruption's tests "can the system absorb two disruptions back to back," not
    "does a single disruption, given a reasonable recovery window, resolve cleanly" — the latter is
    this test's actual intent for each disruption individually. Falls back to the drives-up
    bookmark if the mount-disruption one is unexpectedly missing (should not happen in a normal
    run — see _sleep_until_min_gap's own no-op/warning fallback for a genuinely missing reference).

    All victims are killed before waiting on any of them, so their pods go down together.
    """
    reference_key = (
        "archive_mount_disrupted_time" if "archive_mount_disrupted_time" in _disruption_timing
        else "archive_drives_up_time"
    )
    _sleep_until_min_gap(reference_key, MIN_SECS_SINCE_PREVIOUS_DISRUPTION, "disrupting a report batch")
    report_hosts = [h for h in env.cta_maintd if "diskreport" in h.name.lower()]
    if not report_hosts:
        pytest.skip("No diskReportArchive/diskReportRetrieve maintd pods found in this environment")

    groups = [
        [h for h in report_hosts if "diskreportarchive" in h.name.lower()],
        [h for h in report_hosts if "diskreportretrieve" in h.name.lower()],
    ]
    victims: list[CtaMaintdHost] = []
    remaining = NUM_REPORT_BATCHES_TO_DISRUPT
    while remaining > 0 and any(groups):
        for group in groups:
            if not group:
                continue
            victims.append(group.pop(random.randrange(len(group))))
            remaining -= 1
            if remaining == 0:
                break

    for victim in victims:
        print(f"Disrupting report batch: killing maintd pod {victim.name}")
        victim.restart(wait_for_restart=False)

    for victim in victims:
        victim.wait_for_host_up()
        print(f"maintd pod {victim.name} restarted")


def test_wait_for_archival(eos_mgm: EosMgmHost, stress_params: StressParams, test_dir: Path) -> None:
    num_missing_files, loss_percent = eos_mgm.wait_for_archival_in_directory(
        archive_dir_path=test_dir,
        check_archive_interval_sec=stress_params.check_archive_interval_sec,
        max_no_progress_intervals=stress_params.max_no_progress_intervals,
    )
    loss_acceptable = loss_percent <= stress_params.max_acceptable_loss_percent
    print(f"Missing files: {num_missing_files}")
    print(f"Loss: {loss_percent:.2f}% (threshold: {stress_params.max_acceptable_loss_percent}%)")

    assert loss_acceptable, f"Too many files lost during archival: {num_missing_files} files missing"


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
    loss_acceptable = loss_percent <= stress_params.max_acceptable_loss_percent
    print(f"Missing retrievals: {num_missing}")
    print(f"Loss: {loss_percent:.2f}% (threshold: {stress_params.max_acceptable_loss_percent}%)")

    assert loss_acceptable, f"Too many files not retrieved: {num_missing} files still on tape"


def test_verify_no_jobs_left_behind(scheduler_postgres: SchedulerPostgresHost, stress_params: StressParams) -> None:
    """The real proof this test exists for: every queue the three failure classes can leave rows
    in must be (close to) empty by the time archival and retrieval both finish.

    - ARCHIVE_FAILED_QUEUE / RETRIEVE_FAILED_QUEUE within max_acceptable_loss_percent of
      total_files: a file lands here only if it permanently exhausted its retry budget, which
      test_wait_for_archival/test_wait_for_retrieval already tolerate up to the same percentage —
      this check applies the SAME tolerance here rather than re-asserting zero on top of a loss
      those tests already accepted.
    - No row anywhere still flagged IS_REPORTING: ResubmitInactiveReportingRoutine actually
      cleared whatever test_disrupt_report_batch left stuck, rather than it rotting forever. This
      stays zero-tolerance — it is a distinct failure mode from lost files, not something the
      loss tolerance above is meant to cover.
    - ARCHIVE_PENDING_QUEUE / RETRIEVE_PENDING_QUEUE empty: nothing is still waiting to be picked
      up by a mount — test_wait_for_archival/test_wait_for_retrieval already stopped waiting, so
      this is a consistency check on top of that, not a separate wait. Also zero-tolerance.
    """
    max_failed = int(stress_params.total_files * stress_params.max_acceptable_loss_percent / 100)
    checks = {
        "ARCHIVE_FAILED_QUEUE": ("SELECT COUNT(*) FROM ARCHIVE_FAILED_QUEUE", max_failed),
        "RETRIEVE_FAILED_QUEUE": ("SELECT COUNT(*) FROM RETRIEVE_FAILED_QUEUE", max_failed),
        "ARCHIVE_PENDING_QUEUE": ("SELECT COUNT(*) FROM ARCHIVE_PENDING_QUEUE", 0),
        "RETRIEVE_PENDING_QUEUE": ("SELECT COUNT(*) FROM RETRIEVE_PENDING_QUEUE", 0),
        "ARCHIVE_ACTIVE_QUEUE stuck IS_REPORTING": (
            "SELECT COUNT(*) FROM ARCHIVE_ACTIVE_QUEUE WHERE IS_REPORTING IS TRUE",
            0,
        ),
        "RETRIEVE_ACTIVE_QUEUE stuck IS_REPORTING": (
            "SELECT COUNT(*) FROM RETRIEVE_ACTIVE_QUEUE WHERE IS_REPORTING IS TRUE",
            0,
        ),
    }
    failures: list[str] = []
    for label, (sql, max_allowed) in checks.items():
        count = int(scheduler_postgres.run_sql(sql).strip())
        print(f"\t{label}: {count} (max allowed: {max_allowed})")
        if count > max_allowed:
            failures.append(f"{label} has {count} row(s), expected at most {max_allowed}")

    assert not failures, (
        "Found leftover rows after the run — the cleanup routines did not fully recover from the "
        "injected failures:\n" + "\n".join(failures)
    )


def test_wait_for_dead_mount_recovery(scheduler_postgres: SchedulerPostgresHost) -> None:
    """A narrower, earlier check than test_verify_no_jobs_left_behind: specifically waits for
    MOUNT_QUEUE_LAST_FETCH to stop naming any of the mounts test_disrupt_archive_mount killed,
    within a bounded window derived from presets/stress-scale-fail-values.yaml's
    userActiveQueueCleanup.ageForCollectionSecs (600 s) plus its poll interval (60 s). Run before
    the final assertion so a timeout here points directly at
    ArchiveInactiveMountActiveQueueRoutine/RetrieveInactiveMountActiveQueueRoutine rather than
    being lumped into test_verify_no_jobs_left_behind's more general failure.
    """
    # 600 s age + 60 s poll + 60 s margin for the pass itself to run and commit.
    timeout_secs = 600 + 60 + 60
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
        f"the inactive-mount cleanup routine did not reclaim the disrupted mount(s) in time"
    )
