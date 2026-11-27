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

Commands require system Python 3.9 or newer.
Only `install` requires uv to prepare the Python development environment.
The installer prepares the shared development environment in `.venv`, the command symlink, Bash completion, and the pre-commit Git hook.
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
    `cta-dev deploy`, `up`, and `all` replace the managed deployment. Combined commands start deleting the old deployment while the build runs, before the build has succeeded. Preserve logs, core dumps, and other evidence before invoking them.

### Deploy existing images

```bash
cta-dev deploy
```

This does not build RPMs or images.
The current deployment in the selected namespace is replaced only if the namespace is managed by CTA tooling.

### Deploy images built by CI

Use an existing image tag to deploy without rebuilding locally:

```bash
cta-dev deploy --cta-image-tag <ci-image-tag>
```

Replace `<ci-image-tag>` with a published CI image tag. This selects the CI registry configured by `dev.ctaImageRegistry` in `project.json`. Override the registry with `--cta-image-registry <registry>` if needed; the expected repository paths remain `<registry>/cta/ctageneric/<service>`.

`--cta-image-tag` and `--cta-image-registry` apply only to `deploy`. An explicit `--cta-image-tag` cannot be combined with an explicit `--cta-version`. Match the scheduler and Oracle settings to the selected images.

### Custom Helm values

Use repeatable `--cta-values`, `--eos-values`, and `--dcache-values` options to override the deployment presets.
Values are applied in command-line order, after the defaults; later files take precedence.
CTA values are shared by the services and administration tools.
Relative paths are resolved from `ci/orchestration/`; use absolute paths for files elsewhere:

```bash
cta-dev deploy \
  --infra-values /path/to/infrastructure-values.yaml \
  --cta-values /path/to/cta-values.yaml
```

`--infra-values` replaces the default local database and scheduler-storage provisioning.
Repeat it to combine infrastructure presets, or use `presets/infra/external.yaml` when neither connection needs local provisioning.
Selecting an external connection in CTA values does not disable local provisioning by itself.
Use the connection Secret options below to select an external connection and omit its default local provisioning and reset.
The dCache and local telemetry options add their own infrastructure presets.

Presets are grouped under `presets/infra/`, `presets/cta/`, and `presets/eos/`.
Apply common stress settings before PostgreSQL stress tuning.
Explicit EOS/dCache image options take precedence over values files.

### CI chart responsibilities

| Release | Chart | Responsibility |
| --- | --- | --- |
| `ci-infra` | `ci-infra` | Disposable PostgreSQL instances, KDC, credentials, connection Secrets, VFS storage, and optional telemetry. |
| `cta-tools` | `cta-tools` | Persistent administration tools, API client, and bootstrap reset Jobs. |
| `cta` | `cta` | Long-running CTA services. |
| `eos` | `eos` | EOS disk system. |
| `store` | `dcache` | dCache disk system, using a database provisioned by `ci-infra`. |

The infrastructure chart accepts named PostgreSQL instances, `secretFiles`, and `persistentVolumeClaims`.
Each PostgreSQL instance is a separate server with one initial database and ephemeral storage: replacing its Pod loses its data.
The local VFS preset requires the `local-path` StorageClass.
See `ci/orchestration/helm/ci-infra/values.yaml` for configuration examples and naming defaults.

The tools chart provides three single-replica Deployments, enabled independently:

| Value | Deployment | Access |
| --- | --- | --- |
| `cli.enabled` | `cta-tools-cli` | CTA frontend API, using JWT/Kerberos credentials. |
| `backend.enabled` | `cta-tools-backend` | Direct catalogue and scheduler access. |
| `liquibase.enabled` | `cta-tools-liquibase` | Catalogue migration tools; migrations run only when explicitly invoked. |

Each workload has its own `resources` map; reset Jobs use `reset.resources`.
CLI and backend workloads share `image` settings and `global.image` overrides; Liquibase uses `liquibase.image`.
The Deployments use `Recreate` to avoid overlapping tools pods during updates.
Use the stable `deployment/cta-tools-<role>` target for commands; generated pod names change when pods are replaced.

During bootstrap, catalogue and scheduler reset Jobs run concurrently and must succeed before CTA services start.
When invoking `create_instance.sh` directly, request resets with `--reset-catalogue` or `--reset-scheduler`.
No resets are requested by default; explicit requests apply equally to local and external connections.
Inspect a reset with `kubectl -n <namespace> logs job/cta-tools-reset-catalogue` or `job/cta-tools-reset-scheduler`.

Bootstrap Jobs run once; their logs remain available until Job deletion.
The persistent tools remain available for administration, for example:

```bash
kubectl -n <namespace> exec deployment/cta-tools-backend -c backend -- \
  cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf
```

### Connect to an external database

Use `--catalogue-secret <name>` or `--scheduler-secret <name>` with `deploy`, `up`, `debug`, or `all`.
The Secrets must exist in the `default` namespace; they are copied into the deployment namespace.
These options select the connection and omit its default local provisioning and automatic reset.
Connections without a Secret option are reset automatically; use `--reset-catalogue` or `--reset-scheduler` to explicitly reset a Secret-selected connection too.
Explicit `--infra-values` files control provisioning independently; they do not change this reset policy.
The scheduler backend follows `--scheduler-type`.

```bash
# Use an existing catalogue and initialize a local PostgreSQL scheduler.
cta-dev deploy --scheduler-type pgsched --catalogue-secret external-catalogue

# Explicitly reset both external connections for a disposable test instance.
cta-dev deploy --scheduler-type pgsched \
  --catalogue-secret ci-catalogue --scheduler-secret ci-scheduler \
  --reset-catalogue --reset-scheduler
```

For a walkthrough using your own catalogue and PostgreSQL scheduler databases, see [Testing with External Databases](external-databases.md).
It covers connection Secrets, local provisioning and explicit resets through `create_instance.sh`.

### Runner connection Secrets

Runner-provided connection Secrets are selected explicitly when invoking `create_instance.sh`:

| Connection | Option | Required files |
| --- | --- | --- |
| Oracle catalogue | `--catalogue-secret cta-oracle-catalogue-connection-string` | `cta-catalogue.conf` |
| External PostgreSQL scheduler | `--scheduler-secret cta-stress-scheduler-connection-string` | `cta-scheduler.conf` |
| Ceph scheduler | `--scheduler-secret cta-ceph-scheduler-connection-string` | `cta-scheduler.conf`, `ceph.conf`, `ceph.client.<id>.keyring` |

Use the same names on each runner, with that runner's connection details.
Keep credentials in connection files and Secrets rather than committed values files.
Ceph uses `rados://<client>@<pool>:<namespace>`; the client must match the keyring identity.
Select the scheduler backend with `--cta-values presets/cta/scheduler/postgres.yaml` or `--cta-values presets/cta/scheduler/ceph.yaml`.
Connection Secret options take precedence over Secret names in values files.
They omit the corresponding default local provisioning; explicit `--infra-values` files remain caller-controlled.
CI requests `--reset-catalogue --reset-scheduler` to initialize both selected connections.

Create source Secrets in `default`; the script checks they are readable and copies them under the same names into the new namespace.
Use repeatable `--copy-secret <name>` options for additional Secrets that do not select a connection.
The deployment namespace cannot be `default` when copying Secrets.
Repeated names are copied only once.
Source Secrets remain in `default` after the deployment is removed.
The infrastructure chart's `secretFiles` setting is only for ephemeral/fake CI credentials, which are also stored in Helm release values; do not use it for actual external resources.

### Validate chart changes

Run the checks without deploying:

```bash
sh ci/checks/check_charts.sh
```

This runs `helm lint` and `helm template`, as does the `lint-helm` CI job.
It requires Helm 3 and access to the chart repositories.
Dependencies are prepared in a temporary copy.
Rendered manifests go to `build/helm-checks/`; an optional argument changes that directory.

Linting and rendering do not establish Kubernetes API validity or runtime behaviour.
Use deployment and system tests when changing service startup, storage, or other runtime behaviour.

### EOS

Override the EOS configuration or image for an EOS-backed test deployment:

```bash
cta-dev deploy --eos-values /path/to/eos-values.yaml
cta-dev deploy --eos-image-repository example/eos --eos-image-tag my-tag
```

### dCache

Select the dCache test deployment:

```bash
cta-dev deploy --with-dcache
cta-dev deploy --with-dcache --dcache-values /path/to/dcache-values.yaml
cta-dev deploy --with-dcache --dcache-image-repository example/dcache --dcache-image-tag my-tag
```

### Other deployment options

```bash
# Deploy the local telemetry stack.
cta-dev deploy --telemetry local

# Publish telemetry to the configured backend.
cta-dev deploy --telemetry central
```

Local telemetry is part of `ci-infra` and uses only namespaced Kubernetes resources.
It includes workload/PVC state and host metrics, but not kubelet or per-container cAdvisor metrics.
Central telemetry uses the configured external backend.

For deployment options beyond those exposed by `cta-dev`, invoke `ci/orchestration/create_instance.sh` directly from `ci/orchestration/`.

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
