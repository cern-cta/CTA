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

The standard stress test requires `mhvtl` virtual tape hardware and a dedicated CI runner with the matching kernel modules and library setup. Virtual stress-drive mode removes that dependency: drives are pure in-process software objects backed by a shared PersistentVolumeClaim (PVC), so the full stress workload — archive, retrieve, tape mount and dismount — runs on any Kubernetes cluster without any tape hardware or kernel modules.

### How it works

`StressDrive` is a subclass of `FakeDrive` that persists tape content to a `.tape` file on a shared PVC between mount/dismount cycles. `MediaChangerFacade` in stress mode simulates a single-arm tape robot that serialises all tape movements through a `changer/` directory, mirroring the single-robot-arm behaviour seen in production libraries.

The shared directory layout under `stressBaseDir` (default `/cta-stress-lib`, mounted from the `cta-fake-tape-library` PVC):

```
config/<lib>/library.toml              — library and media-type configuration
contents/<lib>/slots/VID.tape          — tape at rest (empty file = blank tape)
contents/<lib>/changer/VID.tape        — tape in transit (robot holds it)
contents/<lib>/changer/.lock           — robot-busy flag (O_CREAT|O_EXCL, POSIX atomic)
contents/<lib>/drives/<drv>/           — empty = no tape loaded
contents/<lib>/drives/<drv>/VID.tape   — tape mounted in this drive
```

The full lifecycle per archive or retrieve session:

1. `MediaChangerFacade::mountTapeReadWrite(vid)` acquires the single changer lock (`changer/.lock` via `O_CREAT|O_EXCL`) and atomically renames `slots/VID.tape → changer/VID.tape → drives/<drv>/VID.tape`, then releases the lock.
2. `DataTransferSession::findDrive()` detects `DriveDevice = stress://` and instantiates a `StressDrive`.
3. `StressDrive::waitUntilReady()` scans `drives/<drv>/` for the mounted `*.tape` file, derives the VID from its filename stem, looks up the tape capacity from `library.toml`, then loads the binary block stream into the in-memory block vector (`m_tape`). An empty file (blank tape) synthesises an ANSI VOL1 label.
4. The transfer session runs entirely in memory via the inherited `FakeDrive` block I/O — no filesystem I/O during the data transfer itself.
5. `StressDrive::unloadTape()` serialises `m_tape` back to the `*.tape` file via an atomic tmp-rename, then clears the in-memory state.
6. `MediaChangerFacade::dismountTape(vid)` acquires the changer lock and atomically renames `drives/<drv>/VID.tape → changer/VID.tape → slots/VID.tape`.

The changer lock ensures that only one tape is ever in transit at a time across all drives and all `taped` pods sharing the PVC — exactly as a real tape library serialises robot-arm movements.

!!! note "Why `drives/<drv>/VID.tape` is empty during active archiving"
    While a tape is actively mounted, the `*.tape` file in the drive directory exists but its on-disk content may lag behind the in-memory state: `saveTape()` only runs inside `unloadTape()` at the **end** of the session. The CTA catalogue marks files as "archived" during the write session — before `unloadTape()` is called — so the catalogue can report successful archiving while the on-disk file still shows its pre-session size. Both are correct and reflect different stages of the same session.

    `VID.tape` is updated atomically (write to `VID.tape.tmp`, then rename) at dismount. If it is absent from `slots/` after all drives have gone idle for several minutes, `unloadTape()` was not called (session aborted) or `saveTape()` threw — check the `taped` pod logs.

### Library configuration (`library.toml`)

All virtual-library parameters live in a single TOML file that is the **single source of truth** — no manual duplication into `test_params.toml` is required:

```toml
[library]
name             = "stress-lib"
changerTimeoutMs = 30000

[[library.mediaTypes]]
name          = "STRESS30M"
capacityBytes = 31457280   # 30 MiB per tape
vidPrefix     = "ST"
mountDelayMs  = 2000       # simulated mechanical load latency
count         = 100        # 100 virtual tapes: ST0001..ST0100
```

The file is written by the `fake-tape-library` Helm chart (rendered from `values.yaml` into a ConfigMap, then copied to the PVC by a pre-populate Job). `StressDrive` reads it at construction to obtain per-media-type `capacityBytes` and `mountDelayMs` via longest-prefix VID matching. `MediaChangerFacade` reads `changerTimeoutMs` from the same file.

The `setup_stress_drives_test.py` Python test reads the file from the mounted PVC via `CtaTapedHost.read_stress_library_conf()` (which parses it with `tomllib`) to obtain the media type names, capacities, VID prefixes, and tape counts. No separate `test_params.toml` entries are needed.

!!! warning "VID length limit"
    `vidPrefix` must be at most 2 characters so that the full VID fits in the 6-character ANSI tape label VSN field. `HeaderChecker::checkVOL1()` compares the label VSN to the catalogue VID; a longer prefix causes every archive and retrieve session to fail.

### Deployment

Before deploying CTA, install the `fake-tape-library` chart to create the PVC and pre-populate it:

```bash
helm upgrade --install fake-tape-library ci/orchestration/helm/fake-tape-library/ \
  --namespace <your-namespace> --wait --wait-for-jobs
```

Then deploy CTA with the stress-drive overlay:

```bash
helm upgrade --install cta ci/orchestration/helm/cta/ \
  -f presets/stress-cta-pgsched-values.yaml \
  -f presets/stress-stressdrives-pgsched-values.yaml
```

`create_instance.sh --no-hardware-drives` runs both steps automatically. No node labelling, `hostPath` volumes, or tmpfs pre-mounting is required.

The overlay:

- Sets `StressMode = yes` in every `taped` pod's configuration, bypassing SCSI device scanning and `rmcd` calls.
- Mounts the `cta-fake-tape-library` PVC into every `taped` pod at `/cta-stress-lib`, so all pods share one tape library regardless of which node they run on.
- Disables `rmcd` entirely (`rmcd.enabled: false`) — `taped` in stress mode never contacts it.
- Generates the virtual drive list (`STRESS0000`…`STRESS{N-1}`) dynamically via `--max-drives N` passed to `create_instance.sh`. The CI default is 20 drives. Each drive produces one `taped` StatefulSet pod.

For multi-node clusters, use a `ReadWriteMany` storage class in the `fake-tape-library` chart values (default is `ReadWriteOnce`, suitable for single-node clusters such as `kind` or `k3s`).

### Catalogue setup

Run the stress-drive setup test after the standard CTA catalogue setup:

```bash
cta-dev test setup                            # standard catalogue setup
cta-dev test --setup stress-drives setup      # virtual tape library setup
```

`setup_stress_drives_test.py` reads `library.toml` from the pod, then registers every declared media type, the stress logical library, and all virtual tape VIDs (`ST0001`…`ST0100` by default). It is automatically skipped when no stress-mode pods are present, so it is safe to include in a general setup run.

### Configuration reference

All stress-drive parameters are set in one place — `library.toml` in the `fake-tape-library` chart values — with one exception (`stressMode`/`stressBaseDir` which activates the code path in `taped`):

| Parameter | Where to set it | Default |
| --- | --- | --- |
| Activate stress mode | `taped.conf.taped.stressMode` (Helm overlay) | `false` |
| Shared PVC mount path | `taped.conf.taped.stressBaseDir` (Helm overlay) | `/cta-stress-lib` |
| Logical library name | `library.name` in `library.toml` | `stress-lib` |
| Changer lock timeout | `library.changerTimeoutMs` in `library.toml` | `30000` ms |
| Media type name | `[[library.mediaTypes]].name` | `STRESS30M` |
| Tape capacity | `[[library.mediaTypes]].capacityBytes` | `31457280` (30 MiB) |
| VID prefix | `[[library.mediaTypes]].vidPrefix` | `ST` |
| Simulated mount latency | `[[library.mediaTypes]].mountDelayMs` | `2000` ms |
| Number of virtual tapes | `[[library.mediaTypes]].count` | `100` |

Multiple media types in the same library are supported by adding more `[[library.mediaTypes]]` blocks.

## Monitor and evaluate

Use the private [CTA CI Stress Test dashboard](https://monit-grafana.cern.ch/d/cew9yi19uk074d/cta-ci-stress-test?orgId=165) (CERN access required). Select the run's `stress-<job-id>` service namespace and time range, with the corresponding scheduler and catalogue filters where applicable.

Correlate metrics with the workload phases in the test log. Compare archive and retrieve throughput, queue growth and draining, and resource or database bottlenecks where metrics are available. Compare equivalent workloads: changes to concurrency, batching, backend, or image versions can affect the result independently of the code being investigated.

The job allows failure, so a green pipeline does not establish that the stress test passed. Check its result and the pytest and pod logs in the `test_logs/<namespace>/` artifacts. Review stalled progress and missing-file counts against the configured completion tolerances; a successful job alone does not establish zero missing files or absence of a performance regression.

When reporting a regression, include the job link and dashboard time range. For service failures and core dumps, follow [Debugging](../tools-and-environment/debugging.md); for test selection and authoring, see [System Tests](system-tests.md).
