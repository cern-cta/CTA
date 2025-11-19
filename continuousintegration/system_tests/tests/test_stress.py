import pytest
from dataclasses import dataclass
import time


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
        # TODO: don't hardcode the instancename here
        archive_directory="/eos/ctaeos/cta/stress",
    )


#####################################################################################################################
# Tests
#####################################################################################################################


# TODO: make the client and disk buffer generic to allow for multiple implementations: one for eos and one for dcache
# Right now we have a bunch of EOS-specific commands in here. For the actual system tests (and somewhat ideally the stress test)
# this should all be generic; i.e. we have an interface with the operations we want to do (create a file, archive a file, check that the file is on tape)
# and then have different implementations for this
# However, for this we need to define a strategy. Should we execute anything at all on the disk buffer? Should everything go over the client?
# How do we ensure we don't end up with leaky abstractions all over the place

# TODO: find a clean way to communicate side-effects between tests? Ideally the system tests are some sort of pipeline:
# - A test produces some side effect (e.g. some files on EOS)
# - The next test depends on this side effect
# Ideally we have a way to clearly communicate what side effects a test is supposed to produce so that the next test can verify some of these side effects (if it depends on them)

# Note that this current implementation assumes we want to stress test CTA; not EOS (hence we skip some of the client->EOS interaction)
# Also, this is just a first initial outline of the implementation. Lots of things to change and cleanup still
# We should define clearly what our target is
# Putting drives down while queueing and then putting them up will stress the scheduler (while the frontend can go at its own rate)
# Do we want to stress test both at the same time?
# Do we want to separate this?
# We need to think about this before building the full test


def test_generate_and_copy_files(env, stress_params):
    disk_instance_name = env.eosmgm[0].instance_name

    # Create an archive directory on eos
    env.eosmgm[0].force_remove_directory(stress_params.archive_directory)
    env.client[0].exec(f"eos root://{disk_instance_name} mkdir {stress_params.archive_directory}")

    # Create a local directory that we will copy a bunch of times to EOS
    print(f"Using the following parameters:")
    print(f"\tNumber of directories: {stress_params.num_dirs}")
    print(f"\tFiles per directory: {stress_params.num_files_per_dir}")
    print(f"\tFile size: {stress_params.file_size}")

    print(f"Creating a single directory locally")
    local_buffer_dir = "/tmp/_buffer"
    env.client[0].exec(f"rm -rf {local_buffer_dir}")
    env.client[0].exec(f"mkdir {local_buffer_dir}")
    env.client[0].exec(
        f"seq 1 {stress_params.num_files_per_dir} | "
        f"xargs -P {stress_params.io_threads} -I{{}} dd if=/dev/urandom of={local_buffer_dir}/file_{{}} bs={stress_params.file_size} count=1 status=none"
    )

    # Copy this directory a bunch of times to EOS
    print(f"Creating multiple directory copies on EOS")
    timer_start = time.time()
    for i in range(0, stress_params.num_dirs):
        destination_dir = f"{stress_params.archive_directory}/dir_{i}"
        env.client[0].exec(f"eos root://{disk_instance_name} mkdir {destination_dir}")
        # now copy that buffer to a bunch of different files
        env.client[0].exec(
            f"xrdcp --parallel {stress_params.io_threads} --recursive {local_buffer_dir}/* root://{disk_instance_name}/{destination_dir}/"
        )
        num_files_copied = int(env.client[0].execWithOutput(f"eos root://{disk_instance_name} ls {destination_dir} | wc -l"))
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
    env.client[0].exec(f"rm -rf {local_buffer_dir}")


def test_wait_for_archival(env, stress_params):
    # After everything has been moved, wait for the archival to complete
    # We execute this directly on the mgm to bypass some networking between pods (should be negligible though)
    archive_dir = f"{stress_params.archive_directory}"
    directories = env.eosmgm[0].execWithOutput(f"eos ls {archive_dir}").splitlines()
    print(f"Waiting for archival in {len(directories)} directories")
    assert len(directories) == stress_params.num_dirs, "Not all directories were copied over to EOS"
    for directory in directories:
        print(f"Checking files are on tape in {archive_dir}/{directory}")
        num_files_in_directory = env.eosmgm[0].execWithOutput(f"eos ls {archive_dir}/{directory} | wc -l")
        # This should be cleaned up more; move these methods to eos host (or to client?)
        archived_files = env.eosmgm[0].execWithOutput(f'eos ls {archive_dir}/{directory} -y | grep "d0::t1" | wc -l')
        while archived_files != num_files_in_directory:
            time.sleep(1)
            print(f"\t  {archived_files}/{num_files_in_directory} files in {directory} archived to tape")
            archived_files = env.eosmgm[0].execWithOutput(
                f'eos ls {archive_dir}/{directory} -y | grep "d0::t1" | wc -l'
            )
        print(f"All files in {archive_dir}/{directory} archived to tape")
