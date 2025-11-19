# Python Tests for CTA

This directory contains the Python system tests for CTA.

## Prerequisites

Set up a virtual environment (recommended):

```sh
python3 -m venv venv
```

Install the project to get all necessary dependencies:

```sh
pip install pytest
```

## Useful commands:

Run a particular test suite:

```sh
pytest tests/test_stress.py --namespace dev
```

To skip the setup (e.g. initialization of the catalogue), add the `--no-setup` flag:

```sh
pytest tests/test_stress.py --namespace dev --no-setup
```

To skip the teardown (e.g. cleaning of EOS, wiping catalogue -> not necessary if the namespace is deleted anyway), add the `--no-teardown` flag:

```sh
pytest tests/test_stress.py --namespace dev --no-teardown
```

To do the teardown at the start (to ensure you can rerun tests without redeploying everything), add the `--clean-start` flag:

```sh
pytest tests/test_stress.py --namespace dev --clean-start
```

Rerun only the failed tests (`--lf, --last-failed`):

```sh
pytest tests/test_stress.py --namespace dev --lf
```

Rerun the failed tests first and then run the rest of the tests (`--ff, --failed-first`):

```sh
pytest tests/test_stress.py --namespace dev --ff
```

See additional available options (in particular `Custom options`):

```sh
pytest --help
```

See [pytest docs](https://docs.pytest.org/en/stable/how-to/cache.html) for other useful commands.

## Note on Test Execution Order

By default in `pytest`, tests are executed (within a file) in the order that they are defined. As such, be careful with moving methods around as some of our system tests rely on previous tests.

## System Test Structure

Below you can find an overview of the file structure of the system test and where things are located.

```
system_tests/
├── helpers/
│ ├── connections/              # Specifies how to interact with a host. Each host has an underlying connection. These can be e.g. via Kubernetes of via ssh.
│ ├── hosts/                    # Contains the definitions for the different hosts in cta (frontend, taped, etc) and the functions that these hosts can do.
│ └──test_env.py                # Contains the definition of the test environment. A test environment is just a collection of remote hosts.
├── tests/
│ ├── remote_scripts/           # Scripts meant to be executed on the hosts themselves (taped, frontend, etc)
│ ├── test_<test_suite1>.py
│ ├── test_<test_suite2>.py
│ ├── ...
│ └── test_<test_suiteN>.py
├── conftest.py                 # Test fixture for the test environment and logic for adding custom commandline options to pytest
├── pytest.ini                  # Generic pytest configuration options
```

## Writing system tests

- Each test case should be small test one thing
- Each test case should be able to run in any order and be fully idempotent
    - This is not always doable. E.g. when testing the full archive-retrieve workflow, it would be horrible for the test runtime to start from scratch in each test. In this case, a limited set of assumptions on the state of the system can be made
- Test cases should ideally be self-contained and not interfere with other tests
    - E.g. if a test needs to be done on some object in the catalogue, create a new object specific to the test (and tear it down after). Don't use an existing object (if possible).
- IF a test case does make assumptions on the state of the system, test those assumptions first before
    - The exception to this is the initial catalogue state
- Each test case should clean up after itself if this is not too expensive
    - If the cleanup is too expensive, ensure the teardown cleans it up
- Use the `env.ctacli` to execute `cta-admin` commands
- Use `env.client` to execute `eos` commands
    - In exceptional situations, the mgm can be used to execute eos commands
    - If possible, do not execute `cta-admin` commands on the client. The idea would be to make this an EOS client only (eventually).
- Favour python logic over bash logic.
    - In general, only execute basic commands using bash
    - Only in cases where performance would take a considerable hit by doing everything in python should you do the logic in bash
- Reusable functions should be defined in the hosts.
    - For example, labeling a tape is an operation that is specific to the taped host, so this is defined as a separate method in `cta_taped_host.py`
- Make functions reusable
    - When writing e.g. an `archive()` function, don't hardcode assumptions on the number of files, file size etc into the function. These should be passed as arguments
- If scripts need to run on the hosts, put them under `tests/remote_scripts/<hostname>/`
- Any files directly under `tests/` MUST be a test file. Anything else goes into a separate (sub)directory.
