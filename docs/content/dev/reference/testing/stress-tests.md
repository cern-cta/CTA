# Stress Tests

## Purpose and runner

Stress tests exercise sustained archive and retrieve workloads with many small files to investigate throughput, bottlenecks, and regressions. They emphasize software and request-processing throughput rather than physical tape-drive bandwidth.

The tests use one powerful, dedicated stress runner. Only one stress job runs at a time, enforced by the `stress-test` resource group. This prevents competing workloads and interference with shared test resources; a queued job may be waiting for another run to finish.

## Run a stress test

Start `stress-test-python` from the GitLab pipeline. It also runs automatically for applicable release tags; see [Release Procedure](../../contributing/maintainers/releases.md#review-ci-results) for release validation and exceptions.

The job deploys an EOS-backed instance in `stress-<job-id>`, resets the catalogue and scheduler, and enables telemetry. It prepares the test resources, generates and archives files, requests retrieval, and checks completion. Afterwards, it collects logs and removes the instance.

!!! note "Retaining an instance"
    Use the `keep-stress-test-namespace` pipeline input when you need the instance for investigation. This skips automatic cleanup but does not reserve the runner after the job ends. Coordinate cleanup before the next stress run, which may otherwise fail because the library is still in use.

Use `EXTRA_PYTEST_ARGS` for test options, including `--test-config <path>`, and `EXTRA_SPAWN_ARGS` for deployment overrides. Test configuration paths are relative to `ci/system_tests/`. Consult the `stress-test-python` definition in `.gitlab/ci/tests-kubernetes.gitlab-ci.yml` for the effective arguments and backend-specific settings.

Running `cta-dev test stress` locally does not automatically reproduce the dedicated runner or its stress configuration. For runner administration, see [CI Maintenance](../../contributing/maintainers/ci-maintenance.md).

## Stress configuration

The stress environment reduces storage and logging overhead while providing enough concurrency to sustain the workload. Virtual tape media use memory-backed storage; see [cta-ci-node-setup](https://gitlab.cern.ch/cta/ci/cta-ci-node-setup#faster-mhvtl-performance) for host preparation.

The following overrides live under `ci/orchestration/presets/` and are applied on top of the normal deployment configuration:

| Configuration | Purpose |
| --- | --- |
| `stress-cta-values.yaml` | Increases frontend catalogue connections and reduces service logging overhead. |
| `stress-cta-pgsched-values.yaml` | Tunes PostgreSQL scheduler connections, tape buffers, and reporting workers and batches so reporting can keep pace with transfers. |
| `stress-eos-values.yaml` | Uses `fast-ssd` storage for EOS FST and QuarkDB data, increases FST concurrency, and configures workflow communication. |

Workload parameters live under `ci/system_tests/config/`. `test_params.toml` defines file counts and sizes, concurrency, batching, progress checks, and completion tolerances. The PostgreSQL scheduler uses `test_pgsched_params.toml`, which enables prequeueing: drives remain down while work accumulates, reducing the effect of request-generation bottlenecks. It also uses a dedicated scheduler configuration maintained on the runner.

Use the configuration and `ci/system_tests/tests/stress_test.py` from the tested revision for exact values. These settings influence performance but do not establish a guaranteed processing rate.

## Monitor and evaluate

Use the private [CTA CI Stress Test dashboard](https://monit-grafana.cern.ch/d/cew9yi19uk074d/cta-ci-stress-test?orgId=165) (CERN access required). Select the run's `stress-<job-id>` service namespace and time range, with the corresponding scheduler and catalogue filters where applicable.

Correlate metrics with the workload phases in the test log. Compare archive and retrieve throughput, queue growth and draining, and resource or database bottlenecks where metrics are available. Compare equivalent workloads: changes to concurrency, batching, backend, or image versions can affect the result independently of the code being investigated.

The job allows failure, so a green pipeline does not establish that the stress test passed. Check its result and the pytest and pod logs in the `test_logs/<namespace>/` artifacts. Review stalled progress and missing-file counts against the configured completion tolerances; a successful job alone does not establish zero missing files or absence of a performance regression.

When reporting a regression, include the job link and dashboard time range. For service failures and core dumps, follow [Debugging](../tools-and-environment/debugging.md); for test selection and authoring, see [System Tests](system-tests.md).
