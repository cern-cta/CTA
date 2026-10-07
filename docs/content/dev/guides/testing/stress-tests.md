# Stress Tests

## Purpose and runner

Stress tests exercise sustained archive and retrieve workloads with many small files to investigate throughput, bottlenecks, and regressions. They emphasize software and request-processing throughput rather than physical tape-drive bandwidth.

The tests use one powerful, dedicated stress runner. Only one stress job runs at a time, enforced by the `stress-test` resource group. This prevents competing workloads and interference with shared test resources; a queued job may be waiting for another run to finish.

## Run a stress test

Start `stress-test` from the GitLab pipeline. It also runs automatically for applicable release tags; see [Release Procedure](../../contributing/maintainers/releases.md#review-ci-results) for release validation and exceptions.

The job deploys an EOS-backed instance in `stress-<job-id>`, resets the catalogue and scheduler, and enables telemetry. It prepares the test resources, generates and archives files, requests retrieval, and checks completion. Afterwards, it collects logs and removes the instance.

!!! note "Retaining an instance"
    Use the `keep-stress-test-namespace` pipeline input when you need the instance for investigation. This skips automatic cleanup but does not reserve the runner after the job ends. Coordinate cleanup before the next stress run, which may otherwise fail because the library is still in use.

Use `EXTRA_PYTEST_ARGS` for test options, including `--test-config <path>`, and `EXTRA_SPAWN_ARGS` for deployment overrides. Test configuration paths are relative to `ci/system_tests/`. Consult the `stress-test` definition in `.gitlab/ci/tests-kubernetes.gitlab-ci.yml` for the effective arguments and backend-specific settings.

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

## Virtual stress-drive mode

The standard stress test requires `mhvtl` virtual tape hardware and a dedicated CI runner with the matching kernel modules and library setup. Virtual stress-drive mode removes that dependency: drives are pure in-process software objects backed by a shared `tmpfs` directory, so the full stress workload — archive, retrieve, tape mount and dismount — runs on any Kubernetes cluster without any tape hardware or kernel modules.

### How it works

`StressDrive` is a subclass of `FakeDrive` that persists tape content to a file (`tape.bin`) under a shared directory instead of keeping it only in memory. This allows multiple drives — in the same `taped` process or in different pods — to exchange tapes exactly as a real tape library does.

The shared directory layout under `stressBaseDir` (default `/dev/shm/cta-stress`):

```
tapes/{VID}/tape.bin    — serialised tape block stream; written on dismount,
                          read on mount; absent means a fresh blank tape
drives/{driveName}      — symlink → ../../tapes/{VID}/
                          created by the media changer on mount,
                          removed on dismount
```

The full lifecycle per archive or retrieve session:

1. The media changer (`MediaChangerFacade` in stress mode) creates `tapes/{VID}/` and symlinks `drives/{driveName} → ../../tapes/{VID}/`.
2. `DataTransferSession::findDrive()` detects `DriveDevice = stress://` and instantiates a `StressDrive`.
3. `StressDrive::waitUntilReady()` resolves the symlink to locate the tape directory, loads `tape.bin` into the in-memory block vector, and synthesises an ANSI VOL1 label if the tape is blank.
4. The transfer session runs entirely in memory via the inherited `FakeDrive` block I/O — no filesystem access during data transfer.
5. `StressDrive::unloadTape()` serialises the block vector back to `tape.bin`, then clears the in-memory state.
6. The media changer removes the symlink.

Because `stressBaseDir` is on `tmpfs` (`/dev/shm`), all tape I/O stays in RAM and no physical tape drives, SCSI devices, or `rmcd` are contacted.

### Deployment

The stress-drive overlay is `ci/orchestration/presets/stress-stressdrives-pgsched-values.yaml`. Combine it with the base stress preset:

```bash
helm upgrade --install cta . \
  -f presets/stress-cta-pgsched-values.yaml \
  -f presets/stress-stressdrives-pgsched-values.yaml
```

The overlay:

- Sets `StressMode = yes` in every `taped` pod's configuration, bypassing SCSI device scanning and `rmcd` calls.
- Mounts the host's `/dev/shm/cta-stress` into every pod via a `hostPath` volume so all pods share one in-memory tape library.
- Adds a **node affinity rule** (`cta-stress-node=true`) that forces all stress-drive pods onto the same Kubernetes node. This is required because `hostPath` volumes are node-local: pods on different nodes would each see an independent copy of the directory and could not exchange tape state. Label one CI node before deploying:

    ```bash
    kubectl label node <node-name> cta-stress-node=true
    ```

    For multi-node deployments, replace the `hostPath` volume with a `ReadWriteMany` PersistentVolumeClaim (NFS, CephFS, etc.) and remove the affinity rule from the overlay.

- Defines the virtual drives (`STRESS0000`…`STRESSnnnn`). Add or remove entries to scale the drive count; each entry produces one `taped` StatefulSet pod.

### Catalogue setup

Run the stress-drive setup test after the standard CTA catalogue setup:

```bash
cta-dev test setup          # standard catalogue setup
cta-dev test --setup stress-drives setup   # virtual tape library setup
```

`setup_stress_drives_test.py` registers the `STRESS1T` media type, the stress logical library, all virtual tape VIDs (generated from `vid_prefix` and `num_tapes` in `test_params.toml`), and calls `mkdir -p` on every taped pod to initialise the `tapes/` and `drives/` subdirectories. It is automatically skipped when no stress-mode pods are present, so it is safe to include in a general setup run.

### Configuration reference

All stress-drive parameters are set in two places that must be kept consistent:

| Parameter | Helm value (`conf.taped.*`) | `test_params.toml` key (`[tests.stress.drives]`) | Default |
| --- | --- | --- | --- |
| Activate stress mode | `stressMode` | — | `false` |
| Shared tmpfs root | `stressBaseDir` | `base_dir` | `/dev/shm/cta-stress` |
| Simulated mount latency | `stressMountDelayMs` | — | `0` ms |
| Number of virtual tapes | — | `num_tapes` | `20` |
| VID prefix | — | `vid_prefix` | `STRS` |
| Logical library name | — | `logical_library` | `stress-lib` |
| Tape capacity (bytes) | — | `tape_capacity_bytes` | `1 TiB` |

The logical library name and drive names in the Helm overlay must match the values in `test_params.toml`. VIDs are formatted as `{vid_prefix}{index:04d}`, e.g. `ST0001`…`ST0020`.

!!! warning "VID length limit"
    `vid_prefix` must be at most 2 characters so that the full VID fits in the 6-character ANSI tape label VSN field. `HeaderChecker::checkVOL1()` compares the label VSN to the catalogue VID; a longer prefix causes every archive and retrieve session to fail.

## Monitor and evaluate

Use the private [CTA CI Stress Test dashboard](https://monit-grafana.cern.ch/d/cew9yi19uk074d/cta-ci-stress-test?orgId=165) (CERN access required). Select the run's `stress-<job-id>` service namespace and time range, with the corresponding scheduler and catalogue filters where applicable.

Correlate metrics with the workload phases in the test log. Compare archive and retrieve throughput, queue growth and draining, and resource or database bottlenecks where metrics are available. Compare equivalent workloads: changes to concurrency, batching, backend, or image versions can affect the result independently of the code being investigated.

The job allows failure, so a green pipeline does not establish that the stress test passed. Check its result and the pytest and pod logs in the `test_logs/<namespace>/` artifacts. Review stalled progress and missing-file counts against the configured completion tolerances; a successful job alone does not establish zero missing files or absence of a performance regression.

When reporting a regression, include the job link and dashboard time range. For service failures and core dumps, follow [Debugging](../tools-and-environment/debugging.md); for test selection and authoring, see [System Tests](system-tests.md).
