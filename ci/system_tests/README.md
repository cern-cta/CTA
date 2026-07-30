# Python Tests for CTA

This directory contains the Python system tests for CTA.

## Prerequisites

Set up a virtual environment (recommended):

```sh
python3 -m venv venv
```

Install the project to get all necessary dependencies:

```sh
pip install -r requirements.txt
```

## Useful commands:

Run a particular test suite:

```sh
pytest tests/stress_test.py --namespace dev
```

To include the setup tests (e.g. initialization of the catalogue), add the `--setup` flag:

```sh
pytest tests/stress_test.py --namespace dev --setup
```

To include the teardown tests (e.g. cleaning EOS and wiping the catalogue), add the `--teardown` flag:

```sh
pytest tests/stress_test.py --namespace dev --teardown
```

To do the teardown at the start (to ensure you can rerun tests without redeploying everything), add the `--teardown-first` flag:

```sh
pytest tests/stress_test.py --namespace dev --teardown-first
```

To include verification tests at the end, add the `--verification` flag:

```sh
pytest tests/stress_test.py --namespace dev --verification
```

Rerun only the failed tests (`--lf, --last-failed`):

```sh
pytest tests/stress_test.py --namespace dev --lf
```

Resume the test flow from the earliest failed test (`--ff, --failed-first`):

```sh
pytest tests/stress_test.py --namespace dev --ff
```

When setup, verification, and teardown are enabled, rerun options apply to the complete ordered flow.
`--lf` selects only failed tests, while `--ff` selects the earliest failed test and everything after it.

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
├── config/                     # Test parameter files.
├── fixtures/                   # Shared pytest fixtures.
├── helpers/
│   ├── connections/            # Connections to Kubernetes or remote hosts.
│   ├── hosts/                  # Interfaces for CTA and disk-system hosts.
│   └── test_env.py             # Test environment and its collection of hosts.
├── tests/
│   ├── remote_scripts/         # Scripts executed on remote hosts.
│   ├── setup/                  # Tests run before the selected test suite.
│   ├── teardown/               # Tests that clean up the test environment.
│   ├── verification/           # Tests run after the selected test suite.
│   └── <test_suite>_test.py    # Selectable system-test suites.
├── conftest.py                 # CLI options and lifecycle collection logic.
└── pytest.ini                  # Common pytest configuration.
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
- Use the `env.cta_cli` to execute `cta-admin` commands
- Use `env.eos_client` to execute `eos` commands
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
