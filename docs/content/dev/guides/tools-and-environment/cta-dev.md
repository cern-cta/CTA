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
| `build` | Build RPMs | Compile after changing source code. |
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

The installer creates the command symlink, installs Bash completion, and prepares the system-test Python environment. Ensure `~/.local/bin` is on `PATH`.

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
| `CTA_DEV_CTA_VERSION` | Base or full version, for example `6-dev` or `6-dev.pgcat.el9`; missing variant/platform suffixes follow the build settings |
| `CTA_DEV_NAMESPACE` | Kubernetes namespace |

Command-line options override `ci/.cta-dev.env`, which overrides the built-in defaults. The file belongs to the worktree selected by the command symlink.

### Custom versions and image tags

Most development builds should use the default base version (`<releaseFamily>-dev`, currently `6-dev`), read from `project.json`.
The scheduler, Oracle support and platform settings determine the full version used in packages, binaries and image tags; the default settings resolve to `6-dev.pgcat.el9`.
When a distinct version is needed, pass it with `--cta-version`.
For example, with the default build settings, this sets package version `6`, release `local-test.pgcat.el9`, and image tag `6-local-test.pgcat.el9`:

```bash
cta-dev up --cta-version 6-local-test
```

This labels the build from your checkout; it does not select a Git tag or change the source revision.

Use `<version>-<suffix>`: the version contains numbers and dots; the suffix contains lowercase letters, numbers, dots, and hyphens.
An explicit variant or platform suffix must match the build settings.
When running `build`, `images`, and `deploy` separately, pass the same version to every stage.
Local image builds require the host `rpm` command and check that every CTA RPM in the package directory matches the resolved version before building images.
If packages are missing or their version differs, rerun `cta-dev build` with the same version and scheduler/Oracle options.
Binary package builds clear their dedicated RPM output directory before rebuilding, keeping the image context limited to the current build.

## Build options

The default build uses the objectstore scheduler and disables Oracle catalogue support.

| Option | Use |
| --- | --- |
| `--scheduler-type pgsched` | Select the PostgreSQL scheduler. |
| `--enable-oracle-support` | Include Oracle support; requires access to the Oracle dependencies. Do not publish images containing Oracle RPMs publicly. |
| `--platform <platform>` | Select a supported target from `project.json`. |
| `--cta-version <version>` | Set the base or full version; the default `6-dev` resolves to `6-dev.pgcat.el9` with default build settings. |
| `--enable-unit-tests` | Run unit tests during the package build. |
| `--cmake-build-type Debug` | Select a debug build. |
| `--enable-address-sanitizer` | Build with AddressSanitizer. |
| `--disable-ccache` | Bypass the compilation cache. |

Keep platform, scheduler, Oracle support, and version choices consistent between stages. Unit-test, CMake, sanitizer, and cache options apply to `build` and combined commands such as `up` and `all`. Run `cta-dev <command> --help` for the complete option list.

`cta-dev` automatically selects package repositories, using CERN internal repositories when configured and reachable and public repositories otherwise. Use `--use-public-repos` to override this selection.

## Deployment options

Run commands from your CTA checkout. The default namespace is `dev`; use `--namespace` to select another instance. These deployments are for development and testing. For a site deployment, see [Operations](../../../ops/index.md).

!!! warning "Replacing an instance"
    `cta-dev deploy`, `up`, and `all` replace the managed deployment and reset its catalogue and scheduler. Combined commands start deleting the old deployment while the build runs, before the build has succeeded. Preserve logs, core dumps, and other evidence before invoking them.

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
  --catalogue-config /path/to/catalogue-values.yaml \
  --scheduler-config /path/to/scheduler-values.yaml \
  --cta-config /path/to/base-values.yaml,/path/to/feature-values.yaml
```

### EOS

Override the EOS configuration or image for an EOS-backed test deployment:

```bash
cta-dev deploy --eos-config /path/to/eos-values.yaml
cta-dev deploy --eos-image-repository example/eos --eos-image-tag my-tag
```

### dCache

Select the dCache test deployment:

```bash
cta-dev deploy --with-dcache
```

### Other deployment options

```bash
# Deploy the local telemetry stack.
cta-dev deploy --local-telemetry

# Publish telemetry to the configured backend.
cta-dev deploy --publish-telemetry

```

`--spawn-options` forwards additional options to `create_instance.sh` when a dedicated `cta-dev` option is unavailable.

Use `--namespace` with deployment commands, including combined commands, to select another instance:

```bash
cta-dev up --scheduler-type pgsched --namespace my-namespace
```

Keep platform, backend, Oracle support, and version settings consistent across build, image, and deployment stages. See [per-worktree defaults](#configure-per-worktree-defaults) for persistent settings.

Use [Working with Development Pods](development-pods.md) to inspect services and logs, or [System Tests](../testing/system-tests.md) for automated validation.

## Incremental builds and recovery

Compilation runs in a persistent container specific to the worktree and platform. Packages and intermediate files remain in `build/<platform>/` in the checkout. The recorded configuration and build-success state determine whether the next build can reuse its CMake configuration.

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

# Force source package installation.
cta-dev build --force-install
```

`--reset` implies `--clean-build-dirs`. It does not remove deployments or unrelated containers.

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
