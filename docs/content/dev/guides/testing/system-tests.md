# System Tests

These Python tests exercise deployed CTA instances. Prepare the environment with [Environment Setup](../../getting-started/environment-setup.md), including `cta-dev install` for the test dependencies. C++ and Python tooling tests are covered separately under [Unit Tests](unit-tests.md).

## Run a suite

Run the `client` suite against your deployed instance:

```bash
cta-dev test client
```

This suite covers a broad range of CTA workflows and takes a few minutes to run. Use `cta-dev test --help` to list suites, or `cta-dev test` to choose interactively.

To rebuild, redeploy, and run the suite in one command:

```bash
cta-dev all client
```

For deployment options and replacement behaviour, see [cta-dev Reference](../tools-and-environment/cta-dev.md#deployment-options).

## Lifecycle and cleanup

For a regular suite, `cta-dev` includes:

```text
setup -> selected suite -> verification -> teardown
```

Setup prepares the catalogue, tapes, and disk-system resources. Verification checks for unexpected errors and core dumps; teardown cleans test resources. Deployment and test preparation aim to establish a known starting state rather than relying only on cleanup from a previous run.

The configuration in `ci/system_tests/pytest.ini` stops execution at the first failure (`-x`). Verification and teardown are ordinary tests in this sequence, so they may not run after a failure. Inspect the retained state before cleaning it up.

Run phases individually for manual testing or cleanup:

```bash
cta-dev test setup
# Perform manual work against the prepared instance.
cta-dev test verification
cta-dev test teardown
```

To clean leftovers before a fresh test flow:

```bash
cta-dev test client --teardown-first
```

On a normal run, this moves teardown before setup; it does not add a second teardown at the end. Run `cta-dev test teardown` afterwards if you need final cleanup.

## Select tests

Place `cta-dev` options before the suite name; arguments after it are forwarded to pytest:

```bash
cta-dev test --namespace my-feature client
cta-dev test client -k test_simple_archive_retrieve
```

The [`-k` filter](https://docs.pytest.org/en/stable/how-to/usage.html#specifying-which-tests-to-run) selects tests within the suite before the lifecycle tests are added. Setup, verification, and teardown still accompany a matching selection. Some workflow tests depend on earlier tests in the suite, so selecting one test is only valid if its prerequisites are satisfied; lifecycle setup alone may not provide them.

## Resume after a failure

Resume against the existing instance, retaining the state from the failed run:

```bash
# Rerun only recorded failures in the selected flow.
cta-dev test client --lf

# Resume at the earliest recorded failure and continue the remaining flow.
cta-dev test client --ff
```

These are CTA-specific rerun semantics applied to the ordered lifecycle. For example, `--ff` after a suite failure continues with the remaining suite tests, verification, and teardown. After a verification failure it resumes there, then runs teardown. With no matching recorded failures, both options select no tests.

The failure record comes from [pytest's local cache](https://docs.pytest.org/en/stable/how-to/cache.html), not from the deployed instance. The upstream rerun behaviour differs from CTA's lifecycle-aware handling described above. Reuse the same suite, configuration, and environment when resuming. After redeployment or cleanup, run the full lifecycle instead of using `--lf` or `--ff`. In particular, use `cta-dev all client` without rerun flags when rebuilding and replacing the instance. During reruns, `--teardown-first` does not prepend cleanup.

## Run pytest directly

Use pytest directly when working on the runner or needing explicit lifecycle control. From the repository root, activate the environment created by `cta-dev install`, then change to the test directory:

```bash
source ci/system_tests/.venv/bin/activate
cd ci/system_tests
python -m pytest tests/client_test.py --namespace dev --setup --verification --teardown
```

Unlike `cta-dev test client`, a direct invocation requires explicit lifecycle flags. Keep this working directory: lifecycle collection and default configuration paths are relative to it.

| Option | Purpose |
| --- | --- |
| `--setup` | Include preparation before the selected tests. |
| `--verification` | Include checks after the selected tests. |
| `--teardown` | Include cleanup at the end. |
| `--teardown-first` | Place cleanup at the start of a normal run. |
| `--test-config <path>` | Select test parameters; the default is `config/test_params.toml`. |
| `--connection-config <path>` | Use a YAML host-connection configuration instead of namespace discovery. Supply this or `--namespace`, not both. |

For connection-file structure, see `TestEnv.from_config` in `helpers/test_env.py`. The `cta-dev` wrapper supplies a namespace, so use direct pytest invocation for `--connection-config`. Run `python -m pytest --help` for available options; see also the [pytest invocation guide](https://docs.pytest.org/en/stable/how-to/usage.html).

## Write system tests

Keep suite files directly under `ci/system_tests/tests/`, named `<suite>_test.py`, so `cta-dev` can discover them. Put supporting code and data in subdirectories. The directory map is in `ci/system_tests/README.md`.

- Test one behaviour at a time. Prefer independent, repeatable tests that create their own resources and do not modify objects owned by other tests.
- Where a workflow shares expensive setup, keep ordering dependencies explicit and check the state the test relies on. Reordering or filtering such tests can break the flow.
- Clean up resources within the test or its fixtures where practical; otherwise, cover them in teardown. Account for interrupted runs when preparing the next run.
- Use shared [pytest fixtures](https://docs.pytest.org/en/stable/how-to/fixtures.html) for test setup and the `env` host interfaces for service operations. Run `cta-admin` through `env.cta_cli`. Use the common `env.disk_client` and `env.disk_instance` interfaces for portable disk-system operations, and integration-specific helpers when testing EOS-specific behaviour.
- Put reusable service operations in the corresponding host helper. Pass parameters such as file count and size rather than hardcoding assumptions.
- Prefer Python for control flow. Use remote shell scripts when needed, for example to avoid excessive remote calls, and place them under `tests/remote_scripts/<hostname>/`.

Validate new tests with their intended lifecycle and backend. If they depend on earlier tests, run that sequence as well as any supported individual selection.

## Investigate failures

Start with the failing assertion and test output. Use [Working with Development Pods](../tools-and-environment/development-pods.md) for inspection commands and [Debugging](../tools-and-environment/debugging.md) for service failures and core dumps. For failures in CI, see [job logs and artifacts](ci/pipelines.md#investigating-ci-failures).
