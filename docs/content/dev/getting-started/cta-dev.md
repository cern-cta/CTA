# Local build, deployment, and test workflow

`cta-dev` is the recommended entry point for building and testing CTA during development. It wraps the lower-level
build, image, orchestration, and system-test scripts in one command:

```text
build RPMs -> build images -> deploy -> system tests
```

## Quick start

Install `cta-dev` once from the worktree, then build and deploy CTA:

```bash
cd ci
./cta-dev.sh install
cd ..

# Build RPMs, build images, and deploy a local instance.
cta-dev up

# Run a system-test suite against the deployed instance.
cta-dev test client

# Or rebuild, redeploy, and test in one command.
cta-dev all client
```

For a shorter edit/build cycle, use `cta-dev build`. To reuse existing RPMs or images, run `cta-dev images` or
`cta-dev deploy` independently. Run `cta-dev --help` or `cta-dev <command> --help` for the authoritative option list.

Each stage can be run independently. The combined commands cover the common end-to-end workflows:

| Command | Stages | Typical use |
| --- | --- | --- |
| `build` | Build RPMs | Compile after changing source code. |
| `images` | Build images | Repackage existing RPMs into service images. |
| `deploy` | Deploy | Replace the development instance with existing images. |
| `test` | Test | Run system tests against the deployed instance. |
| `up` | Build, images, deploy | Refresh a complete development instance. |
| `debug` | Debug build, images, deploy | Reproduce and inspect a crash. |
| `all` | Build, images, deploy, test | Run the complete workflow. |

## First-time setup

Run the installer from a CTA Git worktree:

```bash
cd ci
./cta-dev.sh install
```

After confirmation, it:

- creates `~/.local/bin/cta-dev`, pointing to this worktree's script;
- installs Bash completion under `~/.local/share/bash-completion/completions/`;
- creates `ci/system_tests/.venv`; and
- installs the system-test Python dependencies (using `uv` when available, otherwise `python3 -m venv` and `pip`).

Restart the shell after installation and make sure `~/.local/bin` is on `PATH`. The symlink belongs to the worktree from
which the installer was run.

### Prerequisites

The script itself requires `curl`, `git`, `jq`, and Python 3. A usable Podman installation is also required; Docker is not supported.
Deploying additionally requires the normal CTA local Kubernetes tooling and a running cluster.
`cta-dev` detects Minikube or k3s when deciding whether it can load newly built images into the cluster.

## Features at a glance

- **Incremental containerized builds.** Compilation runs in a persistent, worktree-specific build container, while
  `build_srpm/` and `build_rpm/` remain in the worktree. Subsequent builds reuse both the container and build output.
- **Configuration-aware build state.** A successful build records its inputs in `build_rpm/.cta-dev-build-state.json`.
  On the next build, `cta-dev` compares the requested configuration with that file and decides whether it can skip
  CMake, must regenerate SRPMs, must clean the build directories, or must recreate the build container. See
  [How incremental build state works](#how-incremental-build-state-works).
- **Repository selection.** CERN internal YUM repositories are used when their definition exists and a short
  reachability check succeeds; otherwise the build automatically falls back to public repositories.
  `--use-public-repos` forces the public repositories.
- **Flexible build variants.** Select the platform, scheduler, Oracle support, CMake generator and build type, ccache,
  AddressSanitizer, unit tests, package version, and debug packages.
- **Parallel service-image builds.** Images are built from the local RPMs, one image per CTA service. They are loaded
  into a detected Minikube or k3s cluster if possible.
- **Safe deployment replacement.** `deploy` refuses to replace an existing namespace unless it is labelled as managed by
  CTA tooling.
- **Deployment variants.** Choose objectstore or PostgreSQL scheduling, EOS or dCache, custom Helm values, a namespace,
  EOS image overrides, and local or published telemetry.
- **Integrated system tests.** Select a suite interactively or by name, forward options to pytest, and use pytest's
  last-failed/failed-first support to shorten repeated test cycles.
- **Crash-debugging workflow.** `debug` builds debuginfo RPMs and a matching `cta-debug` image, deploys them, and
  prints a ready-to-edit `kubectl debug` command. The debug container shares access to core dumps in `/var/log/tmp`.
- **Per-worktree configuration and completion.** Store stable defaults in `ci/.cta-dev.env`; command-line arguments take
  precedence. Installed Bash completion covers commands, options, test suites, and values files.
- **Timing summary.** Combined workflows report the duration of each completed build, image, and deployment stage.

## Configure per-worktree defaults

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
| `CTA_DEV_CTA_VERSION` | Version string containing letters, numbers, dots, and hyphens, for example `5-dev` |
| `CTA_DEV_NAMESPACE` | Kubernetes namespace |

Command-line options overwrite their corresponding env setting if supplied.

## Custom versions and image tags

Most development builds should use the default version and tag (`5-dev`). When a distinct version is needed, pass the
complete value with `--cta-version`. For example, this produces `5.11.20.0-1.test` for both the RPM version and the Docker
image tag:

```bash
cta-dev up --cta-version 5.11.20.0-1.test
```

The version accepts letters, numbers, dots, and hyphens. When running `build`, `images`, and `deploy` separately, pass the
same version to every stage.

## Common workflows

### Build RPMs

```bash
cta-dev build
```

The first invocation creates the build container and SRPM/RPM directories. A normal subsequent invocation reuses them
and, when the recorded configuration still matches, skips the CMake configuration step.

Run the unit tests as part of the build:

```bash
cta-dev build --enable-unit-tests
```

Useful build variants include:

```bash
# Build for the PostgreSQL scheduler backend.
cta-dev build --scheduler-type pgsched

# Build with Oracle catalogue support.
cta-dev build --enable-oracle-support

# Build with AddressSanitizer.
cta-dev build --enable-address-sanitizer

# Select another CMake configuration.
cta-dev build --cmake-build-type Debug

# Bypass ccache for this build.
cta-dev build --disable-ccache
```

### Rebuild Container Images

After a successful RPM build:

```bash
cta-dev images
```

The RPM source is `build_rpm/RPM/RPMS/x86_64`. If neither Minikube nor k3s is installed, `cta-dev` warns that loading
into Kubernetes was skipped.

### Deploy existing images

```bash
cta-dev deploy
```

This does not build RPMs or images. The current deployment in the selected namespace is replaced only if the namespace
is managed by CTA tooling.

Supply custom Helm values (paths are passed to the orchestration scripts):

```bash
cta-dev deploy \
  --catalogue-config my-catalogue-values.yaml \
  --scheduler-config my-scheduler-values.yaml \
  --cta-config base-values.yaml,feature-values.yaml \
  --eos-config my-eos-values.yaml
```

Other examples:

```bash
# Deploy dCache instead of EOS.
cta-dev deploy --with-dcache

# Deploy the local telemetry stack.
cta-dev deploy --local-telemetry

# Publish telemetry to the configured backend.
cta-dev deploy --publish-telemetry

# Override the EOS image.
cta-dev deploy --eos-image-repository example/eos --eos-image-tag my-tag

# Forward extra words to create_instance.sh.
cta-dev deploy --spawn-options "--some-option value"
```

### Build & Deploy

`cta-dev up` is equivalent to `cta-dev build` -> `cta-dev images` -> `cta-dev deploy`.

Build RPMs, build images, and deploy them:

```bash
cta-dev up
```

Use a different backend and namespace:

```bash
cta-dev up --scheduler-type pgsched --namespace my-namespace
```

### Build & Deploy & Test

`cta-dev all` is equivalent to `cta-dev build` -> `cta-dev images` -> `cta-dev deploy` -> `cta-dev test`.

With no test name, `all` opens the same interactive selector as `cta-dev test`:

```bash
cta-dev all
```

Specify a suite for a non-interactive run, which is normally more convenient for repeated development cycles:

```bash
cta-dev all client
```

The selected regular suite includes setup, verification, and teardown, just as it does with `cta-dev test`. Arguments
after the suite name are forwarded to pytest, while `cta-dev` options must appear before the suite name:

```bash
# Rebuild and deploy into my-feature, then run one matching client test.
cta-dev all --namespace my-feature client -k test_simple_archive_retrieve

# Rebuild and deploy, then resume the client flow at its previous failure.
cta-dev all client --ff
```

Use `up` instead when you want a fresh instance but intend to test or inspect it manually. Use `all` when the goal is a
single command that validates the source through packaging, images, deployment, and system tests.

## System tests

System tests require the virtual environment created by `cta-dev install` and a deployed instance. List the current
suite names with `cta-dev test --help`.

Choose interactively:

```bash
cta-dev test
```

Run a named suite:

```bash
cta-dev test client
```

For a regular suite, `cta-dev` automatically includes the complete lifecycle:

```text
setup -> selected suite -> verification -> teardown
```

Run an individual lifecycle phase when preparing or inspecting the deployment manually:

```bash
cta-dev test setup
# Perform manual work against the prepared instance.
cta-dev test verification
cta-dev test teardown
```

Everything after the suite name is passed directly to pytest:

```bash
# Select one test.
cta-dev test client -k test_simple_archive_retrieve

# Clean leftovers before starting the normal lifecycle.
cta-dev test client --teardown-first

# Pass the target namespace to cta-dev before the suite name.
cta-dev test --namespace my-feature client
```

### Resume after a failure

Use `--lf` to run only tests that failed in the previous invocation:

```bash
cta-dev test client --lf
```

Use `--ff` to start at the previous failure and continue with the rest of the ordered lifecycle:

```bash
cta-dev test client --ff
```

For example, a suite failure resumes with the failed/remaining suite tests, verification, and teardown. A verification
failure resumes within verification and then runs teardown. A teardown failure resumes within teardown. If there is no
recorded failure for the selected flow, `--lf` and `--ff` select no tests.

### Build a debugging environment

Run this *before* reproducing a crash, because it replaces the current deployment and its ephemeral core-dump volumes:

```bash
cta-dev debug
```

This is equivalent to `up`, except that it also builds debuginfo packages and the matching `cta-debug` image. It retains
the configured CMake build type (`RelWithDebInfo` by default). After deployment, follow the printed template to choose a
pod and application container and attach an ephemeral debugger:

```bash
kubectl -n dev debug pod/${POD} -it \
  --target=${CONTAINER} \
  --container=cta-debugger \
  --image=localhost/cta/ctageneric/cta-debug:5-dev \
  --profile=general \
  --custom=ci/orchestration/debug/cta-debug-profile.yaml \
  -- bash
```

Core dumps are visible at `/var/log/tmp` in the debugger container and can be opened with `gdb` and the corresponding
executable.

## How incremental build state works

The build container and the build directories solve different problems:

- The persistent container retains installed build dependencies and is named
  after the worktree path and target platform, so separate worktrees will get unique build containers.
  It ensures CTA can be compiled independently of the platform of the host machine.
- `build_srpm/` and `build_rpm/` are bind-mounted from the worktree, keeping
  artifacts available on the host and across container invocations.
- `build_rpm/.cta-dev-build-state.json` records the build-state schema, platform,
  scheduler, Oracle support, generator, CMake build type, ccache, debug-package
  and unit-test choices, AddressSanitizer, CTA version and suffix, XRootD SSI
  version, parallel job count, and repository mode.

On the next build, `cta-dev` compares every recorded value with the requested configuration:

| Change detected | Automatic action |
| --- | --- |
| No change and `CMakeCache.txt` exists | Reuse output and pass `--skip-cmake`. |
| Scheduler, Oracle support, generator, or platform changed | Clean `build_srpm/` and `build_rpm/`, rebuild SRPMs, and reinstall them. |
| Internal/public repository mode changed | Recreate the build container and clean/rebuild both build directories. |
| Another recorded build input changed | Warn about the change and rerun the necessary build configuration without automatically cleaning everything. |
| Build output exists but state is missing, invalid, or from another schema | Clean both build directories and regenerate the state. |
| Recorded `build_srpm/` is missing | Clean and rebuild both build directories. |

The state file is written only after a successful RPM build. This prevents a failed build from being recorded as a
reusable configuration.

### Manual recovery controls

The automatic behavior should handle ordinary option changes. Use these when recovering from stale or corrupted local
state:

```bash
# Clean build_srpm/ and build_rpm/, then regenerate and reinstall SRPMs.
cta-dev build --clean-build-dirs

# Also remove and recreate the persistent build container.
cta-dev build --reset

# Reinstall the current SRPMs without otherwise resetting the build.
cta-dev build --force-install
```

`--reset` implies `--clean-build-dirs`. It does not remove deployments or unrelated containers.

## Troubleshooting

**`cta-dev` is not found after installation**

Restart the shell and verify that `~/.local/bin` is in `PATH`. Alternatively, invoke `ci/cta-dev.sh` directly.

**The wrong worktree is being built**

The installed command is a symlink to the worktree used during installation. Run `readlink -f ~/.local/bin/cta-dev` to
inspect it, then run `./ci/cta-dev.sh install` from the intended worktree.

**Images build but are not available in Kubernetes**

Automatic image loading occurs only when `cta-dev` detects Minikube or k3s. Confirm that the desired local cluster
tooling is installed and running, then rerun `cta-dev images`.

**Deployment is refused because the namespace is not managed by CTA**

This is a safety check. Inspect the namespace before taking action. If it is safe to remove, delete it manually as
instructed by the error and deploy again; `cta-dev` will not delete an unowned namespace for you.

**A build behaves as though old configuration is still present**

First review the change warnings printed by `cta-dev`. If automatic state handling does not recover it, use
`--clean-build-dirs`; use `--reset` only when the persistent build container must also be recreated.
