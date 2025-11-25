import pytest
from dataclasses import dataclass
import time
from collections import deque

from ..helpers.utils.timeout import Timeout
from ..helpers.hosts.disk.disk_instance_host import DiskInstanceHost
from ..helpers.hosts.disk.eos_client_host import EosClientHost

# Most likely we will need to specify a directory and file pre/postfix here
@dataclass
class StressParams:
    num_dirs: int
    num_files_per_dir: int
    # File size in bytes
    file_size: int
    # Path to an archive directory on EOS
    archive_directory: str
    io_threads: int


@pytest.fixture
def stress_params(request):
    return StressParams(
        num_dirs=request.config.getoption("--stress-num-dirs"),
        num_files_per_dir=request.config.getoption("--stress-num-files-per-dir"),
        file_size=request.config.getoption("--stress-file-size"),
        io_threads=8,
        # TODO: don't hardcode the directory here; what if dcache is used?
        # Can we figure out this automatically somehow
        archive_directory="/eos/ctaeos/cta/stress",
    )


#####################################################################################################################
# Tests
#####################################################################################################################


# TODO: find a clean way to communicate side-effects between tests? Ideally the system tests are some sort of pipeline:
# - A test produces some side effect (e.g. some files on EOS)
# - The next test depends on this side effect
# Ideally we have a way to clearly communicate what side effects a test is supposed to produce so that the next test can verify some of these side effects (if it depends on them)


def test_hosts_present(env):
    # Need at least a disk instance and a client
    assert len(env.disk_instance) > 0
    assert len(env.disk_client) > 0
    assert len(env.ctafrontend) > 0
    assert len(env.ctataped) > 0


def test_generate_and_copy_files(env, stress_params):
    disk_instance: DiskInstanceHost = env.disk_instance[0]
    # For now this is an eos client
    # We should factor out all the exec() into dedicated methods for disk_client_host.py
    eos_client: EosClientHost = env.eos_client[0]
    disk_instance_name = disk_instance.instance_name

    # Create an archive directory on eos
    disk_instance.force_remove_directory(stress_params.archive_directory)
    eos_client.exec(f"eos root://{disk_instance_name} mkdir {stress_params.archive_directory}")

    # Create a local directory that we will copy a bunch of times to EOS
    print(f"Using the following parameters:")
    print(f"\tNumber of directories: {stress_params.num_dirs}")
    print(f"\tFiles per directory: {stress_params.num_files_per_dir}")
    print(f"\tFile size: {stress_params.file_size}")

    # For performance reasons, we should probably stick with the old stress script (just make it a lot smaller and focused)
    print(f"Creating a single directory locally")
    local_buffer_dir = "/dev/shm/_buffer"
    eos_client.exec(f"rm -rf {local_buffer_dir}")
    eos_client.exec(f"mkdir {local_buffer_dir}")
    eos_client.exec(
        f"seq 1 {stress_params.num_files_per_dir} | "
        f"xargs -P {stress_params.io_threads} -I{{}} dd if=/dev/urandom of={local_buffer_dir}/file_{{}} bs={stress_params.file_size} count=1 status=none"
    )

    # Copy this directory a bunch of times to EOS
    print(f"Creating multiple directory copies on EOS")
    timer_start = time.time()
    for i in range(0, stress_params.num_dirs):
        destination_dir = f"{stress_params.archive_directory}/dir_{i}"
        eos_client.exec(f"eos root://{disk_instance_name} mkdir {destination_dir}")
        # now copy that buffer to a bunch of different files
        eos_client.exec(
            f"xrdcp --parallel {stress_params.io_threads} --recursive {local_buffer_dir}/* root://{disk_instance_name}/{destination_dir}/"
        )
        num_files_copied = int(eos_client.execWithOutput(f"eos root://{disk_instance_name} ls {destination_dir} | wc -l"))
        assert num_files_copied == stress_params.num_files_per_dir, f"Some files failed to copy over to {destination_dir}"
        print(
            f"\tDir {i + 1}/{stress_params.num_dirs}: copy to {destination_dir} completed"
        )

    # Some stats
    timer_end = time.time()
    duration_seconds = timer_end - timer_start
    total_file_count = stress_params.num_files_per_dir * stress_params.num_dirs
    total_megabytes = total_file_count * stress_params.file_size / 1e6
    avg_mbps = total_megabytes / duration_seconds
    avg_fps = total_file_count / duration_seconds
    print(f"Copy complete:")
    print(f"\tTotal Files: {total_file_count}")
    print(f"\tTotal MB:    {total_megabytes}")
    print(f"\tFiles/s:     {avg_fps:.2f}")
    print(f"\tMB/s:        {avg_mbps:.2f}")
    # And remove the buffer again
    eos_client.exec(f"rm -rf {local_buffer_dir}")


# After everything has been moved, wait for the archival to complete
# We execute this directly on the mgm to bypass some networking between pods (should be negligible though)
def test_wait_for_archival(env, stress_params):
    # Eventually we should split this functionality into a more generic method
    # For now I'll keep it here, but the next few variables would be the arguments
    timeout_secs = 300
    archive_dir = f"{stress_params.archive_directory}"
    num_dirs = stress_params.num_dirs
    disk_instance: DiskInstanceHost = env.disk_instance[0]

    # Few problems to solve here:
    # Not all files might be archived for whatever reason (it's a stress test; errors are bound to happen)
    # Additionally, we have no guarantee on the order in which CTA archives the directories
    # So we can't wait for directories to complete one by one and instead, we continuously loop through all of them as they progress.
    # We don't want to check directories that have completed, so we use a queue

    # TODO: split methods for num files and num directories
    directories = disk_instance.num_files_in_directory(archive_dir)
    assert len(directories) == num_dirs, "Not all directories were copied over to EOS"

    # Construct our queue of all directories we need to check
    total_files_to_archive: int = 0
    # For tracking progress; how many files are left for each directory
    num_files_left: dict[str, int] = {}
    q = deque()
    for directory in directories:
        full_path: str = f"{archive_dir}/{directory}"
        total_files_to_archive += disk_instance.num_files_in_directory(full_path)
        num_files_left[full_path] = disk_instance.num_files_on_disk_only(full_path)
        q.append(full_path)

    print(f"Waiting for archival in {len(directories)} directories")
    # TODO: have some sort of overall progress statistic
    with Timeout(timeout_secs) as t:
        # As long as the queue is not empty, keep checking the directories in there
        while q and not t.expired:
            directory = q.popleft()
            num_files_in_directory = disk_instance.num_files_in_directory(directory)
            archived_files = disk_instance.num_files_on_tape_only(directory)
            # Don't call the num_files_on_disk_only as this is expensive; just do the math here
            num_files_left[directory] = num_files_in_directory - archived_files

            print(f"\t  {archived_files}/{num_files_in_directory} files in {directory} archived to tape")
            if archived_files != num_files_in_directory:
                # Not everything was archived -> put it back in the queue
                q.append(directory)
                # Prevent hammering EOS too hard
                # Only do this when the check failed; if everything is done in a directory, we don't want to needlessly wait
                time.sleep(1)
            else:
                print(f"All files in {directory} archived to tape")

            print(f"\t{sum(num_files_left.values())} files remaining to be archived to tape")

    # At this point either everything was archived or the timeout was reached
    missing_archives: int = 0
    if q:
        print("Timeout reached")
        while q:
            directory = q.popleft()
            missing_archives += disk_instance.num_files_on_disk_only(directory)
        print(f"{missing_archives} files failed to archive")

    print(f"{total_files_to_archive - missing_archives}/{total_files_to_archive} files archived to tape")
