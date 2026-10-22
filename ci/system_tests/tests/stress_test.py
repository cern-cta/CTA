# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import asyncio
import contextlib
import math
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from system_tests.helpers.hosts import CtaCliHost, EosClientHost, EosMgmHost
from system_tests.helpers.test_config import TestConfig
from system_tests.helpers.test_env import TestEnv


@dataclass
class PrequeueParams:
    enabled: bool
    num_files_to_put_drives_up: int
    timeout_to_put_drives_up_sec: int
    check_interval_sec: int


# Most likely we will need to specify a directory and file pre/postfix here
@dataclass
class StressParams:
    total_files: int
    num_files_per_dir: int
    # File size in bytes
    file_size: int
    io_threads: int
    batch_size: int
    check_copy_interval_sec: int
    write_files_in_chunks: bool
    check_archive_interval_sec: int
    max_no_progress_intervals: int
    max_acceptable_loss_percent: float
    prequeue: PrequeueParams  # archive pre-queue threshold
    retrieve_prequeue: PrequeueParams  # retrieve pre-queue threshold (see retrieve_prequeue_fraction)
    # Derived — set in __post_init__
    num_dirs: int = field(init=False)

    def __post_init__(self) -> None:
        if self.num_files_per_dir > 100000:
            raise ValueError(f"num_files_per_dir too high: {self.num_files_per_dir}, max allowed value is 100000")
        self.num_dirs = math.ceil(self.total_files / self.num_files_per_dir)
        if self.num_dirs > 50000:
            raise ValueError(f"num_dirs derived as {self.num_dirs} exceeds the max allowed value of 50000")


@pytest.fixture(scope="module")
def stress_params(test_config: TestConfig, request: pytest.FixtureRequest) -> StressParams:
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

    retrieve_prequeue_fraction = prequeue_config["retrieve_prequeue_fraction"]
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
    cta_cli.exec(
        "cta-admin mp ch --name ctasystest --minarchiverequestage 100 --minretrieverequestage "
        '100 --comment "Longer min ages"'
    )
    # Decrease the logging level
    eos_mgm.exec('eos debug warning "*"')

    # EOS WFE thread-pool and interval tuning.
    #
    # wfe.ntx controls a SINGLE shared thread pool for ALL workflow event types
    # (archive PrepareToArchive, retrieve PrepareToGet/STAGE, disk-report
    # mgm.event=migrated, etc.) — EOS has no per-event-type pool separation.
    # With interval=1 s the scheduler dispatches at most wfe.ntx events per
    # second, so the hard throughput ceiling is wfe.ntx events/s.
    #
    # Why ntx=3000 (not 1500):
    #   During concurrent archive pre-queuing AND drives running, the pool
    #   must serve two simultaneous streams:
    #     - archive WFE events (xrootd_archive.py → EOS → CTA WFE): ~1.5 kHz
    #     - disk-report callbacks (maintd mgm.event=migrated → EOS MGM): ~1 kHz
    #   With ntx=1500 the archive stream saturates the entire pool; disk-report
    #   callbacks get leftover slots and are throttled to ~300 Hz.  Setting
    #   ntx=3000 gives each stream its own headroom and lets both run at full
    #   speed simultaneously.
    #
    # RETRIEVE RATE DEGRADATION — NOT fixable by increasing ntx:
    #   During retrieve pre-queuing (drives DOWN), each fs.prepare(STAGE) call
    #   creates a WFE event that EOS queues under /eos/proc/workflow/q/.  The
    #   WFE scheduler scans that directory every wfe.interval seconds to find
    #   the next batch of events to dispatch.  With drives DOWN nothing
    #   completes, so the directory only grows.  At 9 M pending entries the
    #   scan dominates and observed throughput falls from 1.5 kHz to ~90 Hz —
    #   a scanner bottleneck, not thread starvation.  More ntx threads cannot
    #   help: the scheduler cannot dispatch events faster than it can find them.
    #   Fix: keep the WFE queue directory bounded by starting drives before the
    #   pending-event count becomes large (lower retrieve_prequeue_fraction so
    #   drives come up and complete events before the directory balloons).
    eos_mgm.exec("eos space config default space.wfe.ntx=3000")
    eos_mgm.exec("eos space config default space.wfe.interval=1")
    # Get number of "booted" filesystems
    result = eos_mgm.exec_with_output("eos fs ls | grep -c 'booted'")
    count_fs = int(result.strip())
    for i in range(1, count_fs + 1):
        eos_mgm.exec(f"eos fs config {i} scaninterval=0")


@pytest.mark.asyncio
async def test_generate_and_copy_files(
    cta_cli: CtaCliHost,
    eos_client: EosClientHost,
    eos_mgm: EosMgmHost,
    stress_params: StressParams,
    test_dir: Path,
    remote_scripts_dir: Path,
) -> None:
    eos_client.copy_to(remote_scripts_dir / "eos_client" / "xrootd_archive.py", Path("/tmp/xrootd_archive.py"))
    eos_client.copy_to(remote_scripts_dir / "eos_client" / "count_files.py", Path("/tmp/count_files.py"))
    # Get the IP of EOS MGM pod and use instead of disk instance name to save DNS lookups
    mgm_ip = eos_mgm.get_ip()
    total_file_count = stress_params.num_files_per_dir * stress_params.num_dirs

    print("Using the following parameters:")
    print(f"\tNumber of directories: {stress_params.num_dirs}")
    print(f"\tFiles per directory: {stress_params.num_files_per_dir}")
    print(f"\tTotal files: {total_file_count}")
    print(f"\tFile size: {stress_params.file_size}")
    print(f"\tIO threads: {stress_params.io_threads}")
    print(f"\tBatch size: {stress_params.batch_size}")
    print(f"\tWrite files in chunks: {stress_params.write_files_in_chunks}")
    print(f"\tPrequeueing: {stress_params.prequeue.enabled}")
    print(f"\tArchive: files to put drives up: {stress_params.prequeue.num_files_to_put_drives_up}")
    print(f"\tArchive: timeout to put drives up: {stress_params.prequeue.timeout_to_put_drives_up_sec}")
    print(f"\tRetrieve: files to put drives up: {stress_params.retrieve_prequeue.num_files_to_put_drives_up}")
    print(f"\tRetrieve: timeout to put drives up: {stress_params.retrieve_prequeue.timeout_to_put_drives_up_sec}")

    if stress_params.prequeue.enabled:
        # Put drives down first — we queue archive jobs and only put drives up
        # after enough files have been written, to avoid the drives outpacing the queueing
        cta_cli.set_all_drives_down()
    else:
        cta_cli.set_all_drives_up()

    for subdir in range(stress_params.num_dirs):
        eos_mgm.mkdir(test_dir / str(subdir))

    # Use persistent XRootD Python client for high throughput on many small files
    # The remote script (xrootd_archive.py) runs inside the client pod and uses
    # multiprocessing with persistent XRootD File objects
    # Start archive as async subprocess — allows us to await completion instead of polling PID
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

    # Track drives_up outside nested function so we can check it after task cancellation
    drives_up = not stress_params.prequeue.enabled  # If not prequeue, drives are already up

    # Will be used to notify the monitoring task that archive is finished
    stop_monitoring = asyncio.Event()

    async def monitor_copy_and_put_drives_up() -> None:
        """Monitor file count and put drives up when threshold reached (for prequeue mode)."""
        nonlocal drives_up
        while not stop_monitoring.is_set():
            num_files_so_far = eos_client.count_files_in_namespace(
                eos_host=mgm_ip,
                dest_dir=test_dir,
                num_dirs=stress_params.num_dirs,
                count_procs=5,
            )
            # num_files_so_far = int(
            #     mgm.exec_with_output(f"eos find -f {test_dir} | wc -l")
            # )
            print(f"\t[copy monitor] {num_files_so_far}/{total_file_count} files created", flush=True)

            if not drives_up:
                if num_files_so_far >= stress_params.prequeue.num_files_to_put_drives_up:
                    print(
                        (
                            f"\tThreshold ({stress_params.prequeue.num_files_to_put_drives_up}) reached — putting "
                            f"drives UP"
                        ),
                        flush=True,
                    )
                    # do not wait for status to be UP as drives will immediately start TRANSFERING
                    cta_cli.set_all_drives_up(wait=False)
                    drives_up = True
                elif time.time() - timer_start > stress_params.prequeue.timeout_to_put_drives_up_sec:
                    print("\tTimeout reached — putting drives UP", flush=True)
                    cta_cli.set_all_drives_up(wait=False)
                    drives_up = True

            sleep_time = (
                stress_params.prequeue.check_interval_sec if not drives_up else stress_params.check_copy_interval_sec
            )
            # We suppress timeout because we will just continue with monitoring in that case
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop_monitoring.wait(), sleep_time)

    # Run archive and monitoring concurrently — no PID polling needed
    monitor_task = asyncio.create_task(monitor_copy_and_put_drives_up())
    exec_result = await archive_future
    stop_monitoring.set()
    await monitor_task

    # If archive finished before threshold was reached, still put drives up
    if stress_params.prequeue.enabled and not drives_up:
        print("\tDisregarding drive-up threshold as it is larger than total files — putting drives UP now")
        cta_cli.set_all_drives_up(wait=False)

    timer_end = time.time()

    # Print archive script output
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

    # Some stats
    duration_seconds = timer_end - timer_start
    total_megabytes = total_file_count * stress_params.file_size / 1e6
    avg_mbps = total_megabytes / duration_seconds
    avg_fps = total_file_count / duration_seconds
    print("Copy complete:")
    print(f"\tTotal Files: {total_file_count}")
    print(f"\tTotal MB:    {total_megabytes}")
    print(f"\tFiles/s:     {avg_fps:.2f}")
    print(f"\tMB/s:        {avg_mbps:.2f}")


# After everything has been moved, wait for the archival to complete
# We execute this directly on the mgm to bypass some networking between pods (should be negligible though)


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
    """Initialize Kerberos credentials for poweruser1 (needed for retrieve)."""
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
        """Monitor CTA retrieve queue depth and put drives up when threshold reached (for prequeue mode)."""
        nonlocal drives_up
        while not stop_monitoring.is_set():
            queued = cta_cli.retrieve_queue_file_count()

            print(f"\t[retrieve monitor] {queued}/{total_file_count} retrieve requests queued in CTA", flush=True)

            if not drives_up:
                if queued >= stress_params.retrieve_prequeue.num_files_to_put_drives_up:
                    print(
                        (
                            f"\tThreshold ({stress_params.retrieve_prequeue.num_files_to_put_drives_up}) reached"
                            f" — putting drives UP"
                        ),
                        flush=True,
                    )
                    cta_cli.set_all_drives_up(wait=False)
                    drives_up = True
                elif time.time() - timer_start > stress_params.retrieve_prequeue.timeout_to_put_drives_up_sec:
                    print("\tTimeout reached — putting drives UP", flush=True)
                    cta_cli.set_all_drives_up(wait=False)
                    drives_up = True

            sleep_time = (
                stress_params.retrieve_prequeue.check_interval_sec
                if not drives_up
                else stress_params.check_copy_interval_sec
            )
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop_monitoring.wait(), sleep_time)

    monitor_task = asyncio.create_task(monitor_retrieve_and_put_drives_up())
    exec_result = await retrieve_future
    stop_monitoring.set()
    await monitor_task

    if stress_params.retrieve_prequeue.enabled and not drives_up:
        print("\tDisregarding drive-up threshold — putting drives UP now")
        cta_cli.set_all_drives_up(wait=False)

    timer_end = time.time()

    if exec_result.stdout:
        print(f"Retrieve script output:\n{exec_result.stdout}")
    if not exec_result.success:
        print("Retrieve process failed")
        print(exec_result.stderr)

    duration_seconds = timer_end - timer_start
    avg_fps = total_file_count / duration_seconds
    print(f"Retrieve request queueing completed in {duration_seconds:.1f}s, files/s: {avg_fps:.2f}")


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
