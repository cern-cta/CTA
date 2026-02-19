# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
import time
from dataclasses import dataclass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from ..helpers.hosts.disk.disk_instance_host import DiskInstanceHost
from ..helpers.hosts.disk.eos_client_host import EosClientHost
from ..helpers.connections.k8s_connection import K8sConnection


# Most likely we will need to specify a directory and file pre/postfix here
@dataclass
class StressParams:
    num_dirs: int
    num_files_per_dir: int
    # File size in bytes
    file_size: int
    io_threads: int
    prequeue: bool
    num_files_to_put_drives_up: int
    check_every_sec: int
    timeout_to_put_drives_up: int


@pytest.fixture
def stress_params(request):
    return StressParams(
        num_dirs=request.config.test_config["tests"]["stress"]["num_dirs"],
        num_files_per_dir=request.config.test_config["tests"]["stress"]["num_files_per_dir"],
        file_size=request.config.test_config["tests"]["stress"]["file_size"],
        io_threads=request.config.test_config["tests"]["stress"]["io_threads"],
        prequeue=request.config.test_config["tests"]["stress"]["prequeue"],
        num_files_to_put_drives_up=request.config.test_config["tests"]["stress"]["num_files_to_put_drives_up"],
        check_every_sec=request.config.test_config["tests"]["stress"]["check_every_sec"],
        timeout_to_put_drives_up=request.config.test_config["tests"]["stress"]["timeout_to_put_drives_up"],
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
def test_setup_xrootd_client(env, request):
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

    # Also prepare eos-fst-0 as a second archive source
    namespace = env.eos_client[0].conn.namespace
    fst_conn = K8sConnection(namespace, "eos-fst-0", "eos-fst")
    fst_host = EosClientHost(fst_conn)
    fst_host.install_xrootd_python()
    fst_host.copyTo(
        str(script_dir / "xrootd_archive.py"),
        "/root/xrootd_archive.py",
    )
    # Set up SSS authentication for FST to authenticate as user1
    # 1. Get the MGM's keytab (has the shared secret)
    # 2. Change u:daemon g:daemon to u:user1 g:eosusers
    # 3. Copy to FST
    mgm_keytab = env.eos_mgm[0].execWithOutput("cat /etc/eos.keytab")
    user1_keytab = mgm_keytab.replace("u:daemon", "u:user1").replace("g:daemon", "g:eosusers")
    # Write user1 keytab to FST via base64 (kubectl cp requires tar which FST may not have)
    import base64
    keytab_b64 = base64.b64encode(user1_keytab.encode()).decode()
    fst_host.exec(f"echo '{keytab_b64}' | base64 -d > /etc/eos.keytab")
    fst_host.exec("chmod 400 /etc/eos.keytab")
    # Ensure no krb5 ticket so XRootD uses SSS
    fst_host.exec("kdestroy 2>/dev/null || true")
    # Store on config so subsequent tests can access it
    request.config.fst_host = fst_host


@pytest.mark.eos
def test_update_setup_for_max_powerrrr(env):
    num_drives: int = len(env.cta_taped)
    env.cta_cli[0].exec(f"cta-admin vo ch --vo vo --writemaxdrives {num_drives} --readmaxdrives {num_drives}")
    env.cta_cli[0].exec(
        'cta-admin mp ch --name ctasystest --minarchiverequestage 100 --minretrieverequestage 100 --comment "Longer min ages"'
    )
    env.eos_mgm[0].exec("eos fs config 1 scaninterval=0")


@pytest.mark.eos
def test_generate_and_copy_files(env, stress_params):
    disk_instance: DiskInstanceHost = env.disk_instance[0]
    archive_directory = env.disk_instance[0].base_dir_path + "/cta/stress"
    # For now this is an eos client (hence we mark all methods here as such)
    # We should factor out all the exec() into dedicated methods for disk_client_host.py
    eos_client: EosClientHost = env.eos_client[0]
    disk_instance_name = disk_instance.instance_name

    # Create an archive directory on eos
    print(f"Cleaning up previous archive directory: {archive_directory}")
    disk_instance.force_remove_directory(archive_directory)
    eos_client.exec(f"eos root://{disk_instance_name} mkdir {archive_directory}")

    total_file_count = stress_params.num_files_per_dir * stress_params.num_dirs

    print("Using the following parameters:")
    print(f"\tNumber of directories: {stress_params.num_dirs}")
    print(f"\tFiles per directory: {stress_params.num_files_per_dir}")
    print(f"\tTotal files: {total_file_count}")
    print(f"\tFile size: {stress_params.file_size}")
    print(f"\tIO threads: {stress_params.io_threads}")
    print(f"\tPrequeueing: {stress_params.prequeue}")
    print(f"\tNumber of files to put drives up: {stress_params.num_files_to_put_drives_up}")
    print(f"\tTimeout to put drives up: {stress_params.timeout_to_put_drives_up}")

    if stress_params.prequeue:
        # Put drives down first — we queue archive jobs and only put drives up
        # after enough files have been written, to avoid the drives outpacing the queueing
        env.cta_cli[0].set_all_drives_down()

    # Use persistent XRootD Python client for high throughput on many small files
    # The remote script (xrootd_archive.py) runs inside the client pod and uses
    # multiprocessing with persistent XRootD File objects
    # Launch archive in background so we can monitor file count and put drives up at threshold
    timer_start = time.time()
    done_file = eos_client.archive_files_xrootd_async(
        eos_host=disk_instance_name,
        dest_dir=archive_directory,
        num_files=total_file_count,
        num_dirs=stress_params.num_dirs,
        num_procs=stress_params.io_threads,
        file_size=stress_params.file_size,
    )
    print(f"Archive started in background (done file: {done_file})")

    if stress_params.prequeue:
        # Poll namespace file count and put drives up once threshold is reached
        drives_up = False
        while eos_client.is_archive_running(done_file):
            if not drives_up:
                num_files_so_far = eos_client.count_files_in_namespace(
                    eos_host=disk_instance_name,
                    dest_dir=archive_directory,
                    num_dirs=stress_params.num_dirs,
                    count_procs=5,
                )
                # num_files_so_far = int(
                #     mgm.execWithOutput(f"eos find -f {archive_directory} | wc -l")
                # )
                print(f"\t[archive monitor] {num_files_so_far}/{total_file_count} files created")
                if num_files_so_far >= stress_params.num_files_to_put_drives_up:
                    print(f"\tThreshold ({stress_params.num_files_to_put_drives_up}) reached — putting drives UP")
                    env.cta_cli[0].set_all_drives_up(
                        wait=False
                    )  # do not wait for status to be UP as drives will immediately start TRANSFERING
                    drives_up = True
                if time.time() - timer_start > stress_params.timeout_to_put_drives_up:
                    env.cta_cli[0].set_all_drives_up(wait=False)
                    drives_up = True
            time.sleep(stress_params.check_every_sec)

        # If archive finished before threshold was reached, still put drives up
        if not drives_up:
            print("\tArchive finished before drive-up threshold — putting drives UP now")
            env.cta_cli[0].set_all_drives_up(wait=False)

    # Wait for archive to finish (poll since `wait` doesn't work across kubectl exec sessions)
    # we use this instead of wait because this process is in another kubectl exec session, we cannot wait on it
    while eos_client.is_archive_running(done_file):
        time.sleep(1)

    timer_end = time.time()

    num_files_copied = int(
        eos_client.execWithOutput(f"eos root://{disk_instance_name} find -f {archive_directory} | wc -l")
    )
    if num_files_copied != total_file_count:
        print(f"Some files failed to copy over, we wanted {total_file_count} and got {num_files_copied}")
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


@pytest.mark.eos
def test_generate_and_copy_files_two_pods(env, stress_params, request):
    """Archive files from two pods in parallel: cta-client-0 and eos-fst-0.

    Each pod archives half the total files with non-overlapping file numbers.
    Compare performance with the single-pod test_generate_and_copy_files.
    """
    disk_instance: DiskInstanceHost = env.disk_instance[0]
    archive_directory = env.disk_instance[0].base_dir_path + "/cta/stress_two_pods"
    eos_client: EosClientHost = env.eos_client[0]
    fst_host: EosClientHost = request.config.fst_host
    disk_instance_name = disk_instance.instance_name

    # Create an archive directory and subdirectories on eos
    # We pre-create them here because the FST pod doesn't have the eos CLI
    print(f"Cleaning up previous archive directory: {archive_directory}")
    disk_instance.force_remove_directory(archive_directory)
    eos_client.exec(f"eos root://{disk_instance_name} mkdir {archive_directory}")
    for i in range(stress_params.num_dirs):
        eos_client.exec(f"eos root://{disk_instance_name} mkdir -p {archive_directory}/{i}")

    total_file_count = stress_params.num_files_per_dir * stress_params.num_dirs
    files_per_pod = total_file_count // 2

    print("Using the following parameters (two-pod mode):")
    print(f"\tNumber of directories: {stress_params.num_dirs}")
    print(f"\tFiles per pod: {files_per_pod}")
    print(f"\tTotal files: {files_per_pod * 2}")
    print(f"\tFile size: {stress_params.file_size}")
    print(f"\tIO threads per pod: {stress_params.io_threads // 2}")

    archive_kwargs = dict(
        eos_host=disk_instance_name,
        dest_dir=archive_directory,
        num_files=files_per_pod,
        num_dirs=stress_params.num_dirs,
        num_procs=stress_params.io_threads // 2,
        file_size=stress_params.file_size,
        skip_mkdir=True,
    )

    timer_start = time.time()

    with ThreadPoolExecutor(max_workers=2) as pool:
        future_client = pool.submit(
            eos_client.archive_files_xrootd_async, file_offset=0, **archive_kwargs
        )
        future_fst = pool.submit(
            fst_host.archive_files_xrootd_async, file_offset=files_per_pod, **archive_kwargs
        )
        done_file_client = future_client.result()
        done_file_fst = future_fst.result()

    print(f"cta-client-0 done file: {done_file_client}")
    print(f"eos-fst-0 done file:    {done_file_fst}")

    # Wait for both archive processes to complete (check for marker files)
    while eos_client.is_archive_running(done_file_client) or fst_host.is_archive_running(done_file_fst):
        time.sleep(1)

    timer_end = time.time()

    expected_total = files_per_pod * 2
    num_files_copied = int(
        eos_client.execWithOutput(f"eos root://{disk_instance_name} find -f {archive_directory} | wc -l")
    )
    if num_files_copied != expected_total:
        print(f"Some files failed to copy over, we wanted {expected_total} and got {num_files_copied}")
    else:
        print(f"All {expected_total} files copied successfully to {archive_directory}")

    # Combined stats
    duration_seconds = timer_end - timer_start
    total_megabytes = expected_total * stress_params.file_size / 1e6
    avg_mbps = total_megabytes / duration_seconds
    avg_fps = expected_total / duration_seconds
    print("Two-pod copy complete:")
    print(f"\tTotal Files: {expected_total}")
    print(f"\tTotal MB:    {total_megabytes}")
    print(f"\tFiles/s:     {avg_fps:.2f}")
    print(f"\tMB/s:        {avg_mbps:.2f}")



# After everything has been moved, wait for the archival to complete
# We execute this directly on the mgm to bypass some networking between pods (should be negligible though)
@pytest.mark.eos
def test_wait_for_archival(env):
    archive_directory = env.disk_instance[0].base_dir_path + "/cta/stress_two_pods"
    # This timeout is rather arbitrary for now
    # This is the max amount of time to wait for all files to be archived
    # This should probably be some function of the total number of files
    timeout_secs = 300
    disk_instance: DiskInstanceHost = env.disk_instance[0]

    num_missing_files = disk_instance.wait_for_archival_in_directory(
        archive_dir_path=archive_directory, timeout_secs=timeout_secs
    )
    # Future tests should work with the number of archived files
    # So missing files should be ignored when e.g. retrieving
    print(f"Missing files: {num_missing_files}")
