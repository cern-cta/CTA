!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# cta-dev Reference

`cta-dev` wraps package builds, service-image builds, development deployments, and system tests. For initial setup, follow [Environment Setup](../../getting-started/environment-setup.md). For the edit/build/test cycle, see [Your First Change](../../getting-started/first-change.md); further guidance is in [System Tests](../testing/system-tests.md), and [Debugging](debugging.md).

For standalone builds without installing the command, see [Building Images & Packages](building-images-and-packages.md).

Run `cta-dev --help` or `cta-dev <command> --help` for the authoritative option list. Without installing the command, invoke `./ci/cta-dev.sh` from the repository root.

## Commands

Each stage can be run independently. The combined commands cover the common end-to-end workflows:

| Command | Stages | Typical use |
| --- | --- | --- |
| `build` | Build RPM or Debian packages | Compile after changing source code. |
| `images` | Build images | Repackage existing RPMs into service images. |
| `deploy` | Deploy | Replace the development instance with existing images. |
| `test` | Test | Run [system tests](../testing/system-tests.md) against the deployed instance. |
| `up` | Build, images, deploy | Refresh a complete development instance. |
| `debug` | TODO: revised debugging workflow | See [Debugger tooling](debugging.md#debugger-tooling). |
| `all` | Build, images, deploy, test | Run the complete workflow. |

## Installation and worktree selection

For machine preparation and initial installation, follow [Environment Setup](../../getting-started/environment-setup.md). From the intended checkout's root, install or update the command with:

```bash
./ci/cta-dev.sh install
```

The installer requires uv and prepares the shared development environment in `.venv`, the command symlink, Bash completion, and the pre-commit Git hook.
Rerun it after dependency changes and ensure `~/.local/bin` is on `PATH`.

The installed command uses the checkout its symlink points to, even when you invoke it from another checkout. Check the target with:

```bash
readlink -f ~/.local/bin/cta-dev
```

To switch worktrees, rerun the installer from the desired checkout, or invoke that checkout's `./ci/cta-dev.sh` directly.

## Configuration

### Configure per-worktree defaults

Copy the example and keep only the settings you want to override:

```bash
cp ci/.cta-dev.env.example ci/.cta-dev.env
```

`ci/.cta-dev.env` accepts the following keys:

| Key | Allowed value or meaning |
| --- | --- |
| `CTA_DEV_SCHEDULER_TYPE` | `objectstore` or `pgsched` |
| `CTA_DEV_ORACLE_SUPPORT` | `true` or `false` |
| `CTA_DEV_INTERNAL_REPOS` | `true` to use internal repos when reachable; `false` to force public repos |
| `CTA_DEV_PLATFORM` | A platform defined in `project.json` |
| `CTA_DEV_BUILD_GENERATOR` | `Ninja` or `Unix Makefiles` |
| `CTA_DEV_CMAKE_BUILD_TYPE` | `Release`, `Debug`, `RelWithDebInfo`, or `MinSizeRel` |
| `CTA_DEV_CTA_VERSION` | Neutral source version, for example `6.1.0.dev1`; unset by default |
| `CTA_DEV_NAMESPACE` | Kubernetes namespace |

Command-line options override `ci/.cta-dev.env`, which overrides the built-in defaults. The file belongs to the worktree selected by the command symlink.

### Custom versions and image tags

Local builds default to `<major>.dev1`, regardless of Git tags or uncommitted changes.
Repeated builds reuse the same local version and image tag.
Package revision and backend/platform suffixes are added automatically.
To override the source version:

```bash
cta-dev up --cta-version 6.1.0.dev1
```

This labels the build from your checkout; it does not select a Git tag or change the source revision.
The value is used unchanged, without adding variant or platform suffixes.
Use `<version>-<release>` with exactly one separating hyphen and two nonempty fields containing letters, numbers, `.`, `_`, `+`, `~`, or `^`.
CI and release tooling enforce their naming conventions separately.
When running `build`, `images`, and `deploy` separately, pass the same version to every stage.

## Build options

| Platform | Scheduler and Oracle support | Package output |
| --- | --- | --- |
| EL9 / EL10 | Objectstore by default; PostgreSQL optional. Oracle disabled by default, optional. | `build/<platform>/RPM/` |
| Debian 13 | PostgreSQL only, selected automatically; no Oracle support. | `build/debian13/DEB/` |

Select the platform when building:

```sh
cta-dev build --platform el10
cta-dev build --platform debian13
```

| Option | Use |
| --- | --- |
| `--scheduler-type pgsched` | Select the PostgreSQL scheduler. |
| `--enable-oracle-support` | Include Oracle support; requires access to the Oracle dependencies. Do not publish images containing Oracle RPMs publicly. |
| `--platform <platform>` | Select a supported target from `project.json`. |
| `--cta-version <version>` | Override the neutral source version; the default is `<major>.dev1`. |
| `--enable-unit-tests` | Run unit tests during the package build. |
| `--cmake-build-type Debug` | Select a debug build. |
| `--sanitizers <mode>` | Select `none` (default), `address`, `undefined`, `address,undefined`, or `thread`. |
| `--disable-sccache` | Bypass the compilation cache. |

Keep platform, scheduler, Oracle support, and version choices consistent between stages. Unit-test, CMake, sanitizer, and cache options apply to `build` and combined commands such as `up` and `all`. Run `cta-dev <command> --help` for the complete option list.

`cta-dev` automatically selects package repositories, using CERN internal repositories when configured and reachable and public repositories otherwise. Use `--use-public-repos` to override this selection.

Use `--sanitizers address,undefined --enable-unit-tests` to build and test with AddressSanitizer and UndefinedBehaviorSanitizer.
Run `--sanitizers thread --enable-unit-tests` separately; ThreadSanitizer cannot be combined with AddressSanitizer.

## Deployment options

Run commands from your CTA checkout. The default namespace is `dev`; use `--namespace` to select another instance. These deployments are for development and testing. For a site deployment, see [Operations](../../../ops/index.md).

!!! warning "Replacing an instance"
    `cta-dev deploy`, `up`, and `all` replace the managed deployment and reset its unprotected catalogue and scheduler. Combined commands start deleting the old deployment while the build runs, before the build has succeeded. Preserve logs, core dumps, and other evidence before invoking them.

### Deploy existing images

```bash
cta-dev deploy
```

This does not build RPMs or images. The current deployment in the selected namespace is replaced only if the namespace
is managed by CTA tooling.

### Deploy images built by CI

Use an existing image tag to deploy without rebuilding locally:

```bash
cta-dev deploy --cta-image-tag <ci-image-tag>
```

Replace `<ci-image-tag>` with a published CI image tag. This selects the CI registry configured by `dev.ctaImageRegistry` in `project.json`. Override the registry with `--cta-image-registry <registry>` if needed; the expected repository paths remain `<registry>/cta/ctageneric/<service>`.

`--cta-image-tag` and `--cta-image-registry` apply only to `deploy`. An explicit `--cta-image-tag` cannot be combined with an explicit `--cta-version`. Match the scheduler and Oracle settings to the selected images.

### Custom Helm values

Relative values-file paths are resolved from `ci/orchestration/`, where the deployment scripts run. Use absolute paths for files elsewhere, for example:

```bash
cta-dev deploy \
  --infra-values /path/to/infrastructure-values.yaml \
  --cta-values /path/to/catalogue-values.yaml \
  --cta-values /path/to/scheduler-values.yaml \
  --cta-values /path/to/feature-values.yaml
```

### CI chart responsibilities

The deployment harness installs infrastructure, persistent tools and CTA services as separate Helm releases.

| Release | Chart | Responsibility |
| --- | --- | --- |
| `ci-infra` | `ci-infra` | Disposable PostgreSQL 17 instances, KDC, test credentials, connection Secrets, VFS storage and optional local telemetry. |
| `cta-tools` | `cta-admin-tools` | Persistent direct database tools (`database.enabled`) and API client (`client.enabled`). |
| `cta` | `cta` | Long-running CTA services. |
| `store` | `dcache` | dCache services consuming the database provisioned by `ci-infra`. |

Prometheus and the OpenTelemetry collector are pinned dependencies of `ci-infra`, disabled by default through `prometheus.enabled` and `otel.enabled`.
`--telemetry local` loads `presets/infra/telemetry.yaml`, enabling and configuring both.
Either dependency can be disabled independently in an override.
They share the infrastructure release lifecycle and readiness wait; there are no separate telemetry releases.
The preset preserves the collector and Prometheus service names used by CTA and scraping.
The local telemetry stack creates only namespaced resources, so namespace deletion removes it without cluster-role cleanup.
Prometheus uses explicit scrape targets for itself, the collector, kube-state-metrics and node exporter, without Kubernetes API credentials or discovery.
Kube-state-metrics watches only the release namespace using a Role and RoleBinding and collects workload and PVC state.
Node exporter retains host CPU/memory metrics for the single-node runner; API-server, kubelet and per-container cAdvisor metrics are not collected.
Persistent-volume reclamation remains a separate cleanup step.
`--telemetry central` still configures the external backend without enabling local telemetry.
Dependency preparation fetches the pinned external charts even when disabled; the other CTA charts retain local-only dependency preparation.

Catalogue and scheduler presets contain consumer settings only: Secret/PVC references and maintenance routines.
Files under `presets/infra/` provision local resources; `presets/cta/` contains consumer settings, and `presets/eos/` contains EOS values.
`create_instance.sh --infra-values <file>` replaces the default local catalogue/VFS provisioning; repeat it to compose infrastructure files.
`cta-dev` exposes the same option and otherwise provisions the local catalogue and its build-selected scheduler.
### Protected connections

Use `cta-dev deploy --protected` or `cta-dev up --protected` to preserve existing catalogue and scheduler state during bootstrap and block automated system tests.
Combine this flag with your connection values and `--infra-values presets/infra/external.yaml` when both connections are external.
Protection does not make running CTA services read-only.

For mixed deployments, set `global.catalogue.protected: true` or `global.scheduler.protected: true` in a `--cta-values` file.
The `--protected` flag sets both values to true.
Automatic development resets skip protected connections; explicit reset flags and Liquibase against a protected catalogue fail validation.
`cta-dev all` rejects protected connections before building or deleting the existing deployment; use `up` instead.
Dedicated external CI databases should remain unprotected so their explicit reset and test workflow continues unchanged.

The tools chart records both protection values in `cta-connection-protection`.
Namespace-based system tests, including direct pytest setup and teardown runs, require this ConfigMap and refuse to run if either connection is protected or the policy cannot be read.
Older deployments must be upgraded or recreated to supply it.
SSH connection-config test environments do not use this Kubernetes policy.
There is no test override; manually invoked administrative commands remain the caller's responsibility.

For external connections, use `presets/infra/external.yaml` and add only the local provisioning files you need.
Changing a catalogue or scheduler consumer preset does not implicitly change infrastructure provisioning.
Both entry points accept repeatable `--cta-values <file>`, `--eos-values <file>` and `--dcache-values <file>` options.
Built-in defaults and telemetry/authentication selections come first; supplied files follow in command-line order, with later files taking precedence.
Explicit EOS/dCache image repository and tag arguments take precedence over values files.
Set `dcache.database.existingSecret` in a dCache values file to override the default `dcache-database` connection Secret.
Infrastructure is the exception: supplying any `--infra-values` files replaces the default local provisioning selection, since infrastructure entries accumulate.
The dCache and local telemetry selections still add their infrastructure presets.
Catalogue and scheduler settings use the same `--cta-values` option; there are no separate backend configuration flags or comma-separated file lists.
Compose `cta/scheduler/postgres.yaml` before `cta/scheduler/postgres-external.yaml` for an external PostgreSQL scheduler.
Compose `cta/tests/stress.yaml` before `cta/tests/stress-postgres.yaml` for PostgreSQL stress tuning.
`ci-infra` provisions named resources without interpreting catalogue, scheduler or disk-system backends.
The infrastructure presets in `ci/orchestration/presets/` request those resources and supply the CTA-specific connection files.
For example, this requests two independent PostgreSQL instances:

```yaml
postgres:
  instances:
    example-a: {}
    example-b:
      database: events
      username: app
```

Each map key becomes the Service and StatefulSet name.
The database and username default to that name; an instance can override them and the shared test password.
Its credentials Secret defaults to `<name>-credentials`, or can be named with `secretName`.
`extraSecretData` adds application-specific files using Helm templates with `.connection.host`, `.connection.database`, `.connection.username`, `.connection.password` and the URL-encoded `.connection.uri`.
The catalogue infrastructure preset uses `cta-catalogue.conf: 'postgresql:{{ .connection.uri }}'`; the PostgreSQL template itself knows nothing about CTA's connection-file format.

`secretFiles` creates named Secrets for arbitrary files, including external database and Ceph connections.
File names and contents support Helm templates.
`persistentVolumeClaims` requests named claims with a size, storage class, access modes and optional annotations.
The dCache infrastructure preset is included only when dCache is selected.
The harness passes consumer files directly to CTA and its administrative tools; infrastructure files stay with `ci-infra`.
It does not read configuration back from the installed infrastructure release.
Scheduler presets include their required `maintd.routines`; the shell does not select additional maintenance presets.
Creation applies the same composed CTA overrides to services and tools, and upgrades pass scheduler overrides to both releases.

The service-facing contract is:

```yaml
global:
  catalogue:
    existingSecret: cta-catalogue-conf
  scheduler:
    backend: postgres
    existingSecret: cta-scheduler-conf
```

The Secrets contain `cta-catalogue.conf` and `cta-scheduler.conf`, respectively, in the connection-file format understood by CTA.
VFS also uses `global.scheduler.existingClaim`.
Ceph bundles `cta-scheduler.conf`, `ceph.conf` and `ceph.client.<id>.keyring` in the same scheduler Secret.
The chart mounts that Secret at `/etc/ceph` as well as mounting the connection file; the connection URL selects the client, pool and namespace.
For external databases, omit the local PostgreSQL instance and reference an existing Secret, or supply the connection file through `secretFiles`.
Oracle and Ceph connections use the same file mechanism; no special provisioning branch is needed in the chart.

Each CI runner has its own Kubernetes cluster with runner Secrets in the `default` namespace.
Provision the same Secret names on every runner, with that runner's connection files:

```bash
kubectl -n default create secret generic cta-ceph-scheduler-conf \
  --from-file=cta-scheduler.conf --from-file=ceph.conf --from-file=ceph.client.runner.keyring
kubectl -n default create secret generic cta-oracle-catalogue-conf --from-file=cta-catalogue.conf
kubectl -n default create secret generic cta-stress-scheduler-conf --from-file=cta-scheduler.conf
```

Create only the Secrets needed by that runner's jobs.
The Ceph connection file contains `rados://<client>@<pool>:<namespace>`; its client must match the keyring filename and identity.
The Oracle catalogue and stress PostgreSQL scheduler files use CTA's native connection-file format.
Keep credentials in these files and Secrets rather than Helm values.
The repository presets reference these fixed Secret names.
External Secret names differ from locally provisioned database Secrets so both can coexist on a runner.
The existing Secret copy loop copies available runner Secrets from `default` into the disposable test namespace before bootstrap.
Provision the required Secrets and file keys before running the corresponding jobs.
Namespace cleanup already preserves `default`; existing test namespaces keep their copied credentials until recreated.
Provision these Secrets before switching runners to this branch; the former `/opt/kubernetes/CTA` values files are no longer read.
CI reset flags still reset the selected external catalogue and scheduler, so each runner's connections must select its own disposable test state.

Legacy host-directory VFS support has been removed.
The local VFS preset requires the `local-path` StorageClass; alternatively, provide a preset referencing another PVC.

The PostgreSQL instances share one template but run independently, with explicit ephemeral data volumes.
Deleting or replacing a database pod loses its data; this chart is for disposable CI environments.
The VFS PVC can use delayed binding, so the harness installs infrastructure without waiting, then installs the tools and optional reset Jobs with `--wait --wait-for-jobs` to provide its first consumers.
Once the tools are ready, a Helm upgrade of infrastructure with `--reuse-values --wait --wait-for-jobs` waits for database services, KDC and credential generation.

The harness installs `cta-tools` once.
The requested catalogue and scheduler reset Jobs run concurrently, and Helm waits for them before starting CTA services.
The tools container starts without contacting CTA services or requiring an initialized schema.
The `--reset-catalogue` and `--reset-scheduler` flags enable `reset.catalogue` and `reset.scheduler` in the admin-tools chart, respectively.
Each script clears old state and initializes it.
Without a reset flag, the corresponding Job is omitted and existing state is left unchanged.
Fresh databases and objectstores need the corresponding reset flags.
Catalogue creation accepts `--catalogue-version`, including `latest`.
Each Job has a 240-second deadline covering both readiness and reset, with retries disabled.
Kubernetes waits for the referenced Secrets and PVCs before starting containers.
For PostgreSQL, an init container waits until a read-only query succeeds with the database credentials before the reset starts.
Externally managed Oracle catalogues must already be available; objectstore resets do not use the PostgreSQL readiness check.
Completed and failed Job pods remain available for log collection until the namespace is deleted or the tools release is upgraded.
For example, use `kubectl -n <namespace> logs job/cta-tools-reset-catalogue` to inspect the catalogue reset.
The upgrade script disables both reset Jobs, removing completed bootstrap Jobs without rerunning them.
For manual Helm upgrades, also set `reset.unprotected=false`, `reset.catalogue=false` and `reset.scheduler=false` before changing the tools image.
A failed Job stops deployment before CTA services are installed.
Command output is captured in the deployment/CI log rather than a separate Kubernetes Job log.
Helm upgrades never execute initialization or resets.

The tools remain available for tests and later administration, which invoke the native CTA commands directly.
For example:

```bash
kubectl -n <namespace> exec deployment/cta-tools-admin -c admin -- \
  cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf
```

The API client remains a separate workload in the same release, configured by the `client` section of the composed CTA values.
The admin and API client workloads share the tools chart's `image` settings, with consistent `global.image` overrides; there is no separate `client.image`.
The infrastructure credential-generation Job also remains in place so an ordinary infrastructure upgrade does not regenerate credentials.
A partially failed credential provisioning run requires recreating the disposable environment.

Enabling `liquibase.enabled` creates a pod exposing `/launch_liquibase.sh` for the schema test's tag/update/rollback sequence.
It uses its own `liquibase.image` and source/destination catalogue versions; tests invoke each command explicitly.
The catalogue schema test uses this mode instead of a separate catalogue-updater chart.

To validate these interfaces without deploying, run `pytest ci/checks/test_ci_charts.py` with Helm available.
The infrastructure and dCache rendering checks need their pinned upstream charts in Helm's repository cache.
Prepare them using `helm dependency update` in temporary copies of `ci-infra` and `dcache`; tests use cached archives without network access.

### EOS

Override the EOS configuration or image for an EOS-backed test deployment:

```bash
cta-dev deploy --eos-values /path/to/eos-values.yaml
cta-dev deploy --eos-image-repository example/eos --eos-image-tag my-tag
cta-dev deploy --with-dcache --dcache-values /path/to/dcache-values.yaml
cta-dev deploy --with-dcache --dcache-image-repository example/dcache --dcache-image-tag my-tag
```

### dCache

Select the dCache test deployment:

```bash
cta-dev deploy --with-dcache
```

### Other deployment options

```bash
# Deploy the local telemetry stack.
cta-dev deploy --telemetry local

# Publish telemetry to the configured backend.
cta-dev deploy --telemetry central

```

`--spawn-options` forwards additional options to `create_instance.sh` when a dedicated `cta-dev` option is unavailable.

Use `--namespace` with deployment commands, including combined commands, to select another instance:

```bash
cta-dev up --scheduler-type pgsched --namespace my-namespace
```

Keep platform, backend, Oracle support, and version settings consistent across build, image, and deployment stages. See [per-worktree defaults](#configure-per-worktree-defaults) for persistent settings.

Use [Working with Development Pods](development-pods.md) to inspect services and logs, or [System Tests](../testing/system-tests.md) for automated validation.

## Incremental builds and recovery

Compilation runs in a persistent container specific to the worktree and platform.
Packages and intermediate files remain in `build/<platform>/` in the checkout.
Build configuration and success state are recorded separately for each platform in `build/.cta-dev-state/<platform>.json`, so switching platforms preserves incremental builds.

Build containers record a hash of the platform's build Dockerfile.
Before reuse, `cta-dev` warns if the Dockerfile changed or the container has no recorded hash, then continues normally.
If needed, recreate the container with `cta-dev build --platform <platform> --refresh-build-container`.

- An unchanged configuration after a successful build reuses the output and skips CMake configuration.
- Changing the scheduler, Oracle support, or build generator cleans the platform's build directory and regenerates source packages.
- Other build-option changes rerun the necessary configuration. Changing repository mode refreshes build dependencies.
- Missing or invalid state with existing output causes a clean rebuild. Failed builds are not treated as successful reusable configurations.

### Manual recovery controls

Use these options when recovering from stale or corrupted local state:

```bash
# Clean build/<platform>/ and regenerate source packages.
cta-dev build --clean-build-dirs

# Also recreate the persistent build container.
cta-dev build --reset

# Recreate the container and rerun CMake while preserving build output.
cta-dev build --refresh-build-container

# Force source package installation.
cta-dev build --force-install
```

`--reset` implies `--clean-build-dirs`. It does not remove deployments or unrelated containers.
`--refresh-build-container` rebuilds the image, recreates the container, reinstalls build dependencies, and reruns CMake without requesting a clean build.
Normal configuration-change checks can still require cleaning the build directory.
Caches stored inside the old container are lost; use `--reset` when toolchain changes require a clean rebuild.

## Troubleshooting

**`cta-dev` is not found after installation**

Restart the shell and verify that `~/.local/bin` is in `PATH`. Alternatively, invoke `ci/cta-dev.sh` directly.

**Images build but are not available in Kubernetes**

Automatic image loading occurs only when `cta-dev` detects Minikube or k3s. Confirm that the desired local cluster
tooling is installed and running, then rerun `cta-dev images`.

**Deployment is refused because the namespace is not managed by CTA**

This is a safety check. Inspect the namespace before taking action. If it is safe to remove, delete it manually as
instructed by the error and deploy again; `cta-dev` will not delete an unowned namespace for you.

**A build behaves as though old configuration is still present**

Review the change warnings printed by `cta-dev`, then follow [Manual recovery controls](#manual-recovery-controls) if automatic recovery is insufficient.
