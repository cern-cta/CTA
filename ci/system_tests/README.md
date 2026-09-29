# CTA System Tests

This directory contains the Python system tests for deployed CTA instances.

For setup, running tests, lifecycle behaviour, and writing new tests, see [System Tests](../../docs/content/dev/guides/testing/system-tests.md).

## Directory structure

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
│   └── <test_suite>_test.py     # Selectable system-test suites.
├── conftest.py                 # CLI options and lifecycle collection logic.
└── pytest.ini                  # Common pytest configuration.
```
