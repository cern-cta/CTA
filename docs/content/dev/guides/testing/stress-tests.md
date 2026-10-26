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

### Why VirtualDrive instead of mhVTL

| Property | mhVTL | VirtualDrive / StressDrive |
| --- | --- | --- |
| Backend | SCSI target (kernel) | Filesystem directory (PVC) |
| Tiny-file latency | High (SCSI + driver) | Minimal (direct file I/O) |
| Drive count per node | Limited by SCSI targets | Unlimited (in-process) |
| CI node setup | Kernel module required | Zero (any k8s node) |
| Mount delay | Fixed, hardware-speed | Configurable / jittered |
| Suitable for | Hardware-fidelity tests | Metadata & scale tests |

### How it works

`StressDrive` is a subclass of `VirtualDrive` that persists tape content to a `.tape` file on a shared PVC between mount/dismount cycles. `MediaChangerFacade` in stress mode simulates a single-arm tape robot that serialises all tape movements through a `changer/` directory, mirroring the single-robot-arm behaviour seen in production libraries.

Each library has its own PVC mounted at `/cta-stress-lib-{i}` (library index `i`). `stressBaseDir` for a drive is derived automatically from its `DriveDevice` URI (`virtual:///cta-stress-lib-{i}`), so no manual config key is needed. The shared directory layout under each `stressBaseDir`:

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
2. `DataTransferSession::findDrive()` detects `DriveDevice = virtual://` and instantiates a `StressDrive`.
3. `StressDrive::waitUntilReady()` scans `drives/<drv>/` for the mounted `*.tape` file, derives the VID from its filename stem, looks up the tape capacity from `library.toml`, then loads the binary block stream into the in-memory block vector (`m_tape`). An empty file (blank tape) synthesises an ANSI VOL1 label.
4. The transfer session runs entirely in memory via `VirtualDrive`'s block I/O — no filesystem I/O during the data transfer itself.
5. `StressDrive::unloadTape()` serialises `m_tape` back to the `*.tape` file via an atomic tmp-rename, then clears the in-memory state.
6. `MediaChangerFacade::dismountTape(vid)` acquires the changer lock and atomically renames `drives/<drv>/VID.tape → changer/VID.tape → slots/VID.tape`.

The changer lock ensures that only one tape is ever in transit at a time across all drives and all `taped` pods sharing the PVC — exactly as a real tape library serialises robot-arm movements.

!!! note "Why `drives/<drv>/VID.tape` is empty during active archiving"
    While a tape is actively mounted, the `*.tape` file in the drive directory exists but its on-disk content may lag behind the in-memory state: `saveTape()` only runs inside `unloadTape()` at the **end** of the session. The CTA catalogue marks files as "archived" during the write session — before `unloadTape()` is called — so the catalogue can report successful archiving while the on-disk file still shows its pre-session size. Both are correct and reflect different stages of the same session.

    `VID.tape` is updated atomically (written to `VID.tape.tmp`, then renamed) at dismount. If it is absent from `slots/` after all drives have gone idle for several minutes, `unloadTape()` was not called (session aborted) or `saveTape()` threw — check the `taped` pod logs.

### Library configuration (`library.toml`)

All virtual-library parameters live in a single TOML file that is the **single source of truth** — no manual duplication into `test_params.toml` is required:

```toml
[library]
name             = "stress-lib-0"    # auto-derived: stress-lib-{i}
changerTimeoutMs = 30000

[[library.mediaTypes]]
name           = "STRESS28MB"
capacityBytes  = 29760080   # ≈ 28.4 MB — exactly 3 × 20 000-file batches
vidPrefix      = "S0"       # auto-derived: S{i}  (2 chars + 4-digit index = 6-char VID)
mountDelayMs   = 15000      # base mount latency; actual delay uniformly random in [base, 2×base]
maxWriteRateHz = 1000       # file-rate ceiling per drive; 0 = write at RAM speed
count          = 180        # virtual tapes: S00001..S00180
```

The chart renders one `library-{i}.toml` key per library into a single ConfigMap; the pre-populate Job copies each key to its respective PVC. `library.name` and `vidPrefix` are **auto-derived from the library index** by the chart — they must not be overridden in `values.yaml`. `StressDrive` reads the file at construction to obtain per-media-type `capacityBytes` and `mountDelayMs` via longest-prefix VID matching. `MediaChangerFacade` reads `changerTimeoutMs` from the same file.

`setup_stress_drives_test.py` reads each library's `library.toml` from the pod (via `CtaTapedHost.read_stress_library_conf()`, which parses it with `tomllib`) to obtain media type names, capacities, VID prefixes, and tape counts. No separate `test_params.toml` entries are needed.

!!! warning "VID length limit"
    `vidPrefix` must be at most 2 characters so that the full VID fits in the 6-character ANSI tape label VSN field. `HeaderChecker::checkVOL1()` compares the label VSN to the catalogue VID; a longer prefix causes every archive and retrieve session to fail.

### Deployment

Use `create_instance.sh --no-hardware-drives` — it deploys the `virtual-tape-library` chart (PVC + prepopulate Job) and then the CTA chart in one step. No node labelling, `hostPath` volumes, or tmpfs pre-mounting is required.

Full example with the PostgreSQL scheduler (the combination used by the CI `stress-test` job):

```bash
cd ci/orchestration/
./create_instance.sh \
  --namespace stress-<id> \
  --cta-image-tag <image-tag> \
  --cta-config presets/stress-cta-pgsched-values.yaml,presets/stress-scale-values.yaml \
  --eos-config presets/stress-eos-values.yaml \
  --catalogue-config presets/dev-catalogue-postgres-values.yaml \
  --scheduler-config /opt/kubernetes/CTA/scheduler/stresstest-scheduler-postgres-values.yaml \
  --no-hardware-drives \
  --max-drives 20
```

`--max-drives N` controls how many virtual drives are created; each drive produces one `taped` StatefulSet pod. Drives are named `S{lib}D{index}` (e.g. `S0D0000`…`S0D0019` for 20 drives in a single library). The CI default is 20. Use `--num-stress-libraries M` to split drives across M independent logical libraries (default 1); see [Multi-library deployment](#multi-library-deployment) below.

??? note "Manual helm commands (for reference)"
    If you need to deploy the two steps separately:

    ```bash
    # Step 1 — create PVC and pre-populate tape files
    helm upgrade --install virtual-tape-library ci/orchestration/helm/virtual-tape-library/ \
      --namespace <your-namespace> --wait --wait-for-jobs

    # Step 2 — deploy CTA with the stress-drive overlay
    helm upgrade --install cta ci/orchestration/helm/cta/ \
      -f presets/stress-cta-pgsched-values.yaml \
      -f presets/stress-scale-values.yaml
    ```

The `stress-scale-values.yaml` overlay:

- Tunes `taped` buffer counts, disk threads, and cache TTLs for the stress workload.
- Disables `rmcd` entirely (`rmcd.enabled: false`) — `taped` in stress mode never contacts it.

The PVC mounts (`extraVolumes`) are **not** in the preset — they are generated dynamically by `create_instance.sh --no-hardware-drives` and merged after the preset, one mount per library (`cta-virtual-tape-library-{i}` at `/cta-stress-lib-{i}`).

For multi-node clusters, use a `ReadWriteMany` storage class in the `virtual-tape-library` chart values (default is `ReadWriteOnce`, suitable for single-node clusters such as `kind` or `k3s`).

### Catalogue setup

Run the stress-drive setup test after the standard CTA catalogue setup:

```bash
cta-dev test setup                            # standard catalogue setup
cta-dev test --setup stress-drives setup      # virtual tape library setup
```

`setup_stress_drives_test.py` reads `library.toml` from one pod per logical library, then registers every declared media type (once, shared across all libraries), each logical library, and all virtual tape VIDs for each library (e.g. `S00001`…`S00180` for library 0, `S10001`…`S10180` for library 1). It is automatically skipped when no stress-mode pods are present, so it is safe to include in a general setup run.

### Configuration reference

All stress-drive parameters are set in the `virtual-tape-library` chart `values.yaml`. Library name and VID prefix are auto-derived from the library index and must not be overridden.

| Parameter | Where to set it | Default |
| --- | --- | --- |
| Number of libraries | `libraryCount` in `values.yaml` | `1` |
| Logical library name | **auto-derived**: `stress-lib-{i}` | — |
| Changer lock timeout | `library.changerTimeoutMs` in `values.yaml` | `30000` ms |
| Media type name | `library.mediaTypes[].name` | `STRESS28MB` |
| Tape capacity | `library.mediaTypes[].capacityBytes` | `29760080` (≈28.4 MB) |
| VID prefix | **auto-derived**: `S{i}` (library index) | — |
| Simulated mount latency base | `library.mediaTypes[].mountDelayMs` | `15000` ms |
| Max write rate | `library.mediaTypes[].maxWriteRateHz` | `1000` files/s |
| Tapes per library | `library.mediaTypes[].count` | `180` |
| PVC name prefix | `pvc.namePrefix` | `cta-virtual-tape-library` |
| PVC size | `pvc.size` | `50Gi` |

Multiple media types in the same library are supported by adding more entries to `library.mediaTypes`.

### Multi-library deployment

By default, all drives share a single logical library (`stress-lib-0`). Pass `--num-stress-libraries M` to `create_instance.sh` to split drives across M independent libraries, each with its own PVC and tape pool. This exercises concurrent mount/dismount across independent robot changers and allows more drives without hitting the single-changer serialisation bottleneck.

```bash
./create_instance.sh \
  --namespace stress-<id> \
  --no-hardware-drives \
  --max-drives 30 \
  --num-stress-libraries 3   # 10 drives per library: stress-lib-0, -1, -2
```

Drive-to-library mapping uses ceiling division; the last library may have fewer drives if `max_drives` is not a multiple of `num_stress_libraries`:

| Library | Drives | VIDs |
| --- | --- | --- |
| `stress-lib-0` | S0D0000…S0D0009 | S00001…S00180 |
| `stress-lib-1` | S1D0000…S1D0009 | S10001…S10180 |
| `stress-lib-2` | S2D0000…S2D0009 | S20001…S20180 |

Drive names use the pattern `S{lib}D{index}`, mirroring the `S{lib}` tape VID prefix so every resource belonging to library N is identifiable at a glance in monitoring and logs.

The chart `libraryCount` value must match `--num-stress-libraries`. `create_instance.sh` passes `--set libraryCount=M` automatically. To deploy the chart manually for M libraries:

```bash
helm upgrade --install virtual-tape-library ci/orchestration/helm/virtual-tape-library/ \
  --namespace <ns> --wait --wait-for-jobs \
  --set libraryCount=3
```

All M PVCs are mounted into **every** taped pod, so any pod can read any library's `library.toml`. `setup_stress_drives_test.py` detects the number of libraries automatically from the pods' `LOGICAL_LIBRARY_NAME` environment variable — no manual configuration is needed.

!!! note "VID prefix limit"
    Auto-derived prefixes `S0`…`S9` support up to 10 libraries. The 2-char prefix plus 4-digit tape index equals 6 characters, the ANSI tape label VSN field maximum. `HeaderChecker::checkVOL1()` enforces this limit; exceeding it causes every archive and retrieve session to fail.

## Monitor and evaluate

Use the private [CTA CI Stress Test dashboard](https://monit-grafana.cern.ch/d/cew9yi19uk074d/cta-ci-stress-test?orgId=165) (CERN access required). Select the run's `stress-<job-id>` service namespace and time range, with the corresponding scheduler and catalogue filters where applicable.

Correlate metrics with the workload phases in the test log. Compare archive and retrieve throughput, queue growth and draining, and resource or database bottlenecks where metrics are available. Compare equivalent workloads: changes to concurrency, batching, backend, or image versions can affect the result independently of the code being investigated.

The job allows failure, so a green pipeline does not establish that the stress test passed. Check its result and the pytest and pod logs in the `test_logs/<namespace>/` artifacts. Review stalled progress and missing-file counts against the configured completion tolerances; a successful job alone does not establish zero missing files or absence of a performance regression.

When reporting a regression, include the job link and dashboard time range. For service failures and core dumps, follow [Debugging](../tools-and-environment/debugging.md); for test selection and authoring, see [System Tests](system-tests.md).
