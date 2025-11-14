import pytest
from dataclasses import dataclass

# Most likely we will need to specify a directory and file pre/postfix here
@dataclass
class StressParams:
    num_files: int
    file_size_bytes: int

@pytest.fixture
def stress_params(request):
    return StressParams(
        num_files=request.config.getoption("--stress-num-files"),
        file_size_bytes=request.config.getoption("--stress-file-size"),
    )

#####################################################################################################################
# Tests
#####################################################################################################################

def test_generate_files(env, stress_params):
    num_files: int = stress_params.num_files
    file_size_bytes: int = stress_params.file_size_bytes
    print(f"Archiving {num_files} files of {file_size_bytes} bytes")
    # First generate the files and copy them  from the client to eos mgm (non-archive directory)

def test_enqueue_archive(env, stress_params):
    # Now move all the files to an archive directory using a simple mv
    ...

def test_wait_for_archival(env, stress_params):
    # After everything has been moved, wait for the archival to complete
    ...
