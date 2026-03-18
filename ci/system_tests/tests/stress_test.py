# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import asyncio
import pytest
import time
from dataclasses import dataclass
from pathlib import Path
import contextlib

from ..helpers.hosts.disk.disk_instance_host import DiskInstanceHost
from ..helpers.hosts.disk.eos_client_host import EosClientHost


@dataclass
class PrequeueParams:
    enabled: bool
    num_files_to_put_drives_up: int
    timeout_to_put_drives_up_sec: int
    check_interval_sec: int


# Most likely we will need to specify a directory and file pre/postfix here
@dataclass
class StressParams:
    num_dirs: int
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
    prequeue: PrequeueParams

    def __post_init__(self):
        # 50k directories and 100k files are the limits eos find applies
        # when truncating the results
        # we do not use EOS find so we do not hit this restriction, but we constrain
        # the inputs similarly all the same
        if self.num_dirs > 50000:
            raise ValueError(f"num_dirs too high: {self.num_dirs}, max allowed value is 50000")
        if self.num_files_per_dir > 100000:
            raise ValueError(f"num_files_per_dir too high: {self.num_files_per_dir}, max allowed value is 100000")


@pytest.fixture
def stress_params(request):
    stress_config = request.config.test_config["tests"]["stress"]
    prequeue_config = stress_config["prequeue"]
    return StressParams(
        num_dirs=stress_config["num_dirs"],
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
            enabled=prequeue_config["enabled"],
            num_files_to_put_drives_up=prequeue_config["num_files_to_put_drives_up"],
            timeout_to_put_drives_up_sec=prequeue_config["timeout_to_put_drives_up_sec"],
            check_interval_sec=prequeue_config["check_interval_sec"],
        ),
    )


#####################################################################################################################
# Tests
#####################################################################################################################


@pytest.mark.eos
def test_hosts_present_stress(env):
    assert len(env.eos_mgm) > 0
    assert len(env.eos_client) > 0
    assert len(env.cta_frontend) > 0
    assert len(env.cta_taped) > 0


@pytest.mark.eos
def test_setup_xrootd_client(env):
    """Install XRootD Python bindings and deploy scripts to the client pod."""
    eos_client: EosClientHost = env.eos_client[0]
    eos_client.install_xrootd_python()

    script_dir = Path(__file__).parent / "remote_scripts" / "eos_client"
    eos_client.copyTo(
        str(script_dir / "xrootd_archive.py"),
        "/root/xrootd_archive.py",
    )
    eos_client.copyTo(
        str(script_dir / "count_files.py"),
        "/root/count_files.py",
    )


@pytest.mark.eos
def test_update_setup_for_max_powerrrr(env):
    num_drives: int = len(env.cta_taped)
    env.cta_cli[0].exec(f"cta-admin vo ch --vo vo --writemaxdrives {num_drives} --readmaxdrives {num_drives}")
    env.cta_cli[0].exec(
        'cta-admin mp ch --name ctasystest --minarchiverequestage 100 --minretrieverequestage 100 --comment "Longer min ages"'
    )
    env.eos_mgm[0].exec("eos fs config 1 scaninterval=0")


@pytest.mark.eos
@pytest.mark.asyncio
async def test_generate_and_copy_files(env, stress_params):
    disk_instance: DiskInstanceHost = env.disk_instance[0]
    archive_directory = env.disk_instance[0].base_dir_path + "/cta/stress"
    # For now this is an eos client (hence we mark all methods here as such)
    # We should factor out all the exec() into dedicated methods for disk_client_host.py
    eos_client: EosClientHost = env.eos_client[0]

    # Get the IP of EOS MGM pod and use instead of disk instance name to save DNS lookups
    mgm_conn = env.eos_mgm[0].conn
    mgm_ip = env.execLocal(
        f"kubectl get pod {mgm_conn.pod} -n {mgm_conn.namespace} -o jsonpath='{{.status.podIP}}'", True
    ).stdout.decode("utf-8")

    # Create an archive directory on eos
    print(f"Cleaning up previous archive directory: {archive_directory}")
    disk_instance.force_remove_directory(archive_directory)
    eos_client.exec(f"eos root://{mgm_ip} mkdir {archive_directory}")

    total_file_count = stress_params.num_files_per_dir * stress_params.num_dirs

    print("Using the following parameters:")
    print(f"\tNumber of directories: {stress_params.num_dirs}")
    print(f"\tFiles per directory: {stress_params.num_files_per_dir}")
    print(f"\tTotal files: {total_file_count}")
    print(f"\tFile size: {stress_params.file_size}")
    print(f"\tIO threads: {stress_params.io_threads}")
    print(f"\tWrite files in chunks: {stress_params.write_files_in_chunks}")
    print(f"\tPrequeueing: {stress_params.prequeue.enabled}")
    print(f"\tNumber of files to put drives up: {stress_params.prequeue.num_files_to_put_drives_up}")
    print(f"\tTimeout to put drives up: {stress_params.prequeue.timeout_to_put_drives_up_sec}")

    if stress_params.prequeue.enabled:
        # Put drives down first — we queue archive jobs and only put drives up
        # after enough files have been written, to avoid the drives outpacing the queueing
        env.cta_cli[0].set_all_drives_down()
    else:
        env.cta_cli[0].set_all_drives_up()

    # Use persistent XRootD Python client for high throughput on many small files
    # The remote script (xrootd_archive.py) runs inside the client pod and uses
    # multiprocessing with persistent XRootD File objects
    # Start archive as async subprocess — allows us to await completion instead of polling PID
    timer_start = time.time()
    archive_proc = await eos_client.start_archive_process_async(
        eos_host=mgm_ip,
        dest_dir=archive_directory,
        num_files=total_file_count,
        num_dirs=stress_params.num_dirs,
        num_procs=stress_params.io_threads,
        file_size=stress_params.file_size,
        batch_size=stress_params.batch_size,
        sss_keytab="/etc/eos.keytab",
        write_files_in_chunks=stress_params.write_files_in_chunks,
    )
    print("Archive process started")

    # Track drives_up outside nested function so we can check it after task cancellation
    drives_up = not stress_params.prequeue.enabled  # If not prequeue, drives are already up

    # Will be used to notify the monitoring task that archive is finished
    stop_monitoring = asyncio.Event()

    async def monitor_copy_and_put_drives_up():
        """Monitor file count and put drives up when threshold reached (for prequeue mode)."""
        nonlocal drives_up
        while archive_proc.returncode is None and not stop_monitoring.is_set():
            num_files_so_far = eos_client.count_files_in_namespace(
                eos_host=mgm_ip,
                dest_dir=archive_directory,
                num_dirs=stress_params.num_dirs,
                count_procs=5,
            )
            # num_files_so_far = int(
            #     mgm.execWithOutput(f"eos find -f {archive_directory} | wc -l")
            # )
            print(f"\t[copy monitor] {num_files_so_far}/{total_file_count} files created", flush=True)

            if not drives_up:
                if num_files_so_far >= stress_params.prequeue.num_files_to_put_drives_up:
                    print(
                        f"\tThreshold ({stress_params.prequeue.num_files_to_put_drives_up}) reached — putting drives UP",
                        flush=True,
                    )
                    # do not wait for status to be UP as drives will immediately start TRANSFERING
                    env.cta_cli[0].set_all_drives_up(wait=False)
                    drives_up = True
                elif time.time() - timer_start > stress_params.prequeue.timeout_to_put_drives_up_sec:
                    print("\tTimeout reached — putting drives UP", flush=True)
                    env.cta_cli[0].set_all_drives_up(wait=False)
                    drives_up = True

            sleep_time = (
                stress_params.prequeue.check_interval_sec if not drives_up else stress_params.check_copy_interval_sec
            )
            # We suppress timeout because we will just continue with monitoring in that case
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop_monitoring.wait(), sleep_time)

    # Run archive and monitoring concurrently — no PID polling needed
    monitor_task = asyncio.create_task(monitor_copy_and_put_drives_up())
    stdout, stderr = await archive_proc.communicate()
    stop_monitoring.set()
    await monitor_task

    # If archive finished before threshold was reached, still put drives up
    if stress_params.prequeue.enabled and not drives_up:
        print("\tDisregarding drive-up threshold as it is larger than total files — putting drives UP now")
        env.cta_cli[0].set_all_drives_up(wait=False)

    timer_end = time.time()

    # Print archive script output
    if stdout:
        print(f"Archive script output: {stdout.decode()}")
    if archive_proc.returncode != 0:
        print(f"Archive process failed with exit code {archive_proc.returncode}")
        if stderr:
            print(stderr.decode())

    num_files_copied = eos_client.count_files_in_namespace(
        eos_host=mgm_ip,
        dest_dir=archive_directory,
        num_dirs=stress_params.num_dirs,
        count_procs=5,
    )
    if num_files_copied != total_file_count:
        print(f"Some files failed to copy over, expected: {total_file_count} and actual: {num_files_copied}")
    else:
        print(f"All {total_file_count} files copied successfully to {archive_directory}")

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
@pytest.mark.eos
def test_wait_for_archival(env, stress_params):
    archive_directory = env.disk_instance[0].base_dir_path + "/cta/stress"
    disk_instance: DiskInstanceHost = env.disk_instance[0]

    num_missing_files, loss_percent = disk_instance.wait_for_archival_in_directory(
        archive_dir_path=archive_directory,
        check_archive_interval_sec=stress_params.check_archive_interval_sec,
        max_no_progress_intervals=stress_params.max_no_progress_intervals,
    )
    loss_acceptable = loss_percent <= stress_params.max_acceptable_loss_percent
    print(f"Missing files: {num_missing_files}")
    print(f"Loss: {loss_percent:.2f}% (threshold: {stress_params.max_acceptable_loss_percent}%)")

    assert loss_acceptable, f"Too many files lost during archival: {num_missing_files} files missing"
