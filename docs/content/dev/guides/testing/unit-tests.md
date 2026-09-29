# Unit Tests

CTA has compiled C++ tests and separate Python tests for tooling. Running the package-build unit tests does not run every test in the repository.

## Run the C++ unit tests

From your checkout, run:

```bash
cta-dev build --enable-unit-tests
```

`cta-dev` builds in its persistent build container and enables `CTA_RUN_UNIT_TESTS`. The RPM build's `%check` phase invokes the CMake target `cta-unit-tests-short`, which runs `cta-unit-tests` followed by `cta-unit-tests-multiprocess`. Without `--enable-unit-tests`, `cta-dev` skips this execution step.

A deployed CTA instance, Kubernetes, and virtual tape hardware are not required. The compiled tests depend on the selected build variant; for example, the PostgreSQL scheduler build starts a temporary PostgreSQL instance for its tests.[^postgres-tests] See [cta-dev build options](../tools-and-environment/cta-dev.md#build-options) for backend selection.

## Test binaries and targets

`tests/CMakeLists.txt` defines both executable targets and targets that run tests. Building an executable target alone does not run it.

| Executable | Scope |
| --- | --- |
| `cta-unit-tests` | Main C++ suite, including common utilities, catalogue, scheduler, and tape-service tests. The included backend tests depend on the build configuration. |
| `cta-unit-tests-multiprocess` | Tests involving subprocesses and multiprocess daemon behaviour. |
| `cta-unit-tests-rdbms` | Database-backed catalogue and connection tests, using a supplied database connection file. These require a dedicated test database and are outside the short target. |

| Run target | What it executes |
| --- | --- |
| `cta-unit-tests-short` | Main and multiprocess suites without dynamic analysis; used by the package build. |
| `cta-unit-tests-valgrind` | Main and multiprocess suites under Valgrind memory checking. |
| `cta-unit-tests-helgrind` | Main and multiprocess suites under Helgrind thread-error checking. |
| `cta-unit-tests-full` | Normal execution plus Valgrind and Helgrind runs. It does not include the separate database or Python suites. |
| `cta-unit-tests-helgrind-parallel` | Groups the filtered Helgrind targets for the base, scheduler, object-store, data-transfer, and in-memory catalogue tests. |

For an existing configured build, invoke a run target inside the build environment with:

```bash
cmake --build <build-directory> --target cta-unit-tests-short
```

Dynamic-analysis targets additionally require Valgrind. Use the definitions in `tests/CMakeLists.txt` for the complete target list and filters. The separate `cta-integration-tests` executable is not included in `cta-unit-tests-short`.

## Select individual C++ tests

The test binaries use GoogleTest and accept its test-listing and filtering options.

!!! note "Individual test selection"
    `cta-dev` currently does not support selecting a C++ unit-test target or forwarding a GoogleTest filter. Selected tests must be run directly through the test binaries in the build environment. `cta-dev test` selects Python system-test suites, not these binaries.

## Python tooling tests

Python tooling has its own tests and dependencies, separate from the C++ package checks. Follow the test instructions for the tool or package you are changing, whether it lives under `ci/` or elsewhere in the repository.

### CI tooling suite

The `unit-test-python` CI job currently runs:

```bash
python -m pytest ci --ignore=ci/system_tests
```

This covers Python tooling tests under `ci/`, including release tooling and documentation publication. It is not repository-wide Python test discovery. The documentation publication tests need the MkDocs/mike environment prepared by `ci/docs/prepare-environment.sh`; consult `.gitlab/ci/tests.gitlab-ci.yml` for the job's current setup.

With the required dependencies installed, run a narrower suite, for example:

```bash
python -m pytest ci/release/tests
```

Tests under `ci/system_tests` exercise deployed CTA instances and have their own setup and execution flow; see [System Tests](system-tests.md). The command above covers only CI tooling; Python tools and libraries elsewhere require their own test invocation. Neither `cta-dev build --enable-unit-tests` nor this CI tooling suite is a repository-wide Python test runner.

[^postgres-tests]: Tests that use a real PostgreSQL instance are technically closer to integration tests. They are included in this suite to avoid overcomplicating the test implementation.
