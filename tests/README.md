# Test infrastructure

This directory contains the shared C++ test runners and support code.
Component test cases live alongside their implementation or in component-local test directories; add new cases to the relevant component's test library in its `CMakeLists.txt`.

| Directory | Contents |
| --- | --- |
| `runners/` | Entry point for the main and multiprocess suites. |
| `runners/rdbms/` | Dedicated database runner, command-line parsing and catalogue factory. |
| `support/` | Shared C++ test utilities, including temporary files, temporary directories and argument vectors. |
| `analysis/` | Dynamic-analysis scripts, Makefile templates and Valgrind, Helgrind and ThreadSanitizer suppressions. |

`CMakeLists.txt` defines the runners and analysis targets and links the component test libraries.
The executables and generated analysis files are written directly into the build tree's `tests/` directory.

The `cta-unit-tests` and `cta-unit-tests-multiprocess` runners run without a deployed CTA instance; PostgreSQL scheduler builds start a temporary PostgreSQL instance.
The separate `cta-unit-tests-rdbms` runner requires a supplied connection file and a dedicated test database.
See [Unit Tests](../docs/content/dev/guides/testing/unit-tests.md) for commands and target coverage.

Tests exercising a deployed CTA instance live in [`ci/system_tests/`](../ci/system_tests/).
See [System Tests](../docs/content/dev/guides/testing/system-tests.md) for their environment and execution workflow.
