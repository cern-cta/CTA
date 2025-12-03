import pytest
import time
from dataclasses import dataclass

from ..helpers.hosts.disk.disk_instance_host import DiskInstanceHost
from ..helpers.hosts.disk.eos_client_host import EosClientHost


# Most likely we will need to specify a directory and file pre/postfix here
@dataclass
class StressParams:
    num_dirs: int
    num_files_per_dir: int
    # File size in bytes
    file_size: int
    io_threads: int


@pytest.fixture
def stress_params(request):
    return StressParams(
        num_dirs=request.config.getoption("--stress-num-dirs"),
        num_files_per_dir=request.config.getoption("--stress-num-files-per-dir"),
        file_size=request.config.getoption("--stress-file-size"),
        io_threads=8,
    )


#####################################################################################################################
# Tests
#####################################################################################################################


def test_hosts_present_stress(env):
    # Need at least a disk instance and a client
    assert len(env.eos_mgm) > 0
    assert len(env.eos_client) > 0
    assert len(env.cta_frontend) > 0
    assert len(env.cta_taped) > 0


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

    # Create a local directory that we will copy a bunch of times to EOS
    print("Using the following parameters:")
    print(f"\tNumber of directories: {stress_params.num_dirs}")
    print(f"\tFiles per directory: {stress_params.num_files_per_dir}")
    print(f"\tFile size: {stress_params.file_size}")

    # For performance reasons, we should probably stick with the old stress script (just make it a lot smaller and focused)
    # For now I'll keep this
    print("Creating a single directory locally")
    local_buffer_dir = "/dev/shm/_buffer"
    eos_client.exec(f"rm -rf {local_buffer_dir}")
    eos_client.exec(f"mkdir {local_buffer_dir}")
    eos_client.exec(
        f"seq 1 {stress_params.num_files_per_dir} | "
        f"xargs -P {stress_params.io_threads} -I{{}} dd if=/dev/urandom of={local_buffer_dir}/file_{{}} bs={stress_params.file_size} count=1 status=none"
    )

    # Copy this directory a bunch of times to EOS
    print("Creating multiple directory copies on EOS")
    timer_start = time.time()
    for i in range(0, stress_params.num_dirs):
        destination_dir = f"{archive_directory}/dir_{i}"
        eos_client.exec(f"eos root://{disk_instance_name} mkdir {destination_dir}")
        # now copy that buffer to a bunch of different files
        eos_client.exec(
            f"xrdcp --parallel {stress_params.io_threads} --recursive {local_buffer_dir}/* root://{disk_instance_name}/{destination_dir}/"
        )
        num_files_copied = int(
            eos_client.execWithOutput(f"eos root://{disk_instance_name} ls {destination_dir} | wc -l")
        )
        assert (
            num_files_copied == stress_params.num_files_per_dir
        ), f"Some files failed to copy over to {destination_dir}"
        print(f"\tDir {i + 1}/{stress_params.num_dirs}: copy to {destination_dir} completed")

    # Some stats
    timer_end = time.time()
    duration_seconds = timer_end - timer_start
    total_file_count = stress_params.num_files_per_dir * stress_params.num_dirs
    total_megabytes = total_file_count * stress_params.file_size / 1e6
    avg_mbps = total_megabytes / duration_seconds
    avg_fps = total_file_count / duration_seconds
    print("Copy complete:")
    print(f"\tTotal Files: {total_file_count}")
    print(f"\tTotal MB:    {total_megabytes}")
    print(f"\tFiles/s:     {avg_fps:.2f}")
    print(f"\tMB/s:        {avg_mbps:.2f}")
    # And remove the buffer again
    eos_client.exec(f"rm -rf {local_buffer_dir}")


# After everything has been moved, wait for the archival to complete
# We execute this directly on the mgm to bypass some networking between pods (should be negligible though)
@pytest.mark.eos
def test_wait_for_archival(env):
    archive_directory = env.disk_instance[0].base_dir_path + "/cta/stress"
    timeout_secs = 300
    disk_instance: DiskInstanceHost = env.disk_instance[0]

    disk_instance.wait_for_archival_in_directory(
        archive_dir_path=archive_directory,
        timeout_secs=timeout_secs,
        strict=False,
    )
