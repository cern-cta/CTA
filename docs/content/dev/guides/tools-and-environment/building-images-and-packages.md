# Building Images & Packages

This self-contained guide is for people who want to build CTA RPMs or container images without setting up a development environment. It invokes the repository’s `cta-dev` script directly, so installing the `cta-dev` command is not required. If you only need RPMs, stop after the package build.

Developers with an established environment should use their installed `cta-dev` command; see [cta-dev Reference](cta-dev.md).

## Requirements

Use a Linux machine with working Podman, Git, Bash, `curl`, `jq`, and Python 3. Docker is not supported by the build scripts. Compilation and build dependencies run inside a container.

Kubernetes and virtual tape hardware are only needed when running a development instance, not for these build stages.

## Get the source

Use your existing CTA checkout, or clone the public repository to build the current development branch:

```bash
git clone https://gitlab.cern.ch/cta/CTA.git
cd CTA
```

Initialize the submodules in your checkout:

```bash
git submodule update --init --recursive
```

The build uses your current branch and local changes.

!!! note "Building a release"
    To build a specific release instead, switch to its tag and update the submodules:

    ```bash
    git switch --detach <release-tag>
    git submodule update --init --recursive
    ```

    Replace `<release-tag>` with the required tag. Use the documentation and scripts from that version; older tags may use different build commands or paths.

Run the remaining commands from the repository root.

## Build RPMs

Build the packages with a custom version:

```bash
./ci/cta-dev.sh build --cta-version 6-local-test
```

`--cta-version 6-local-test` sets the package version to `6` and its release suffix to `local-test`; the complete value also becomes the container image tag. It labels the build from your current checkout and does not select a Git tag or change the source being built. Use the same value when building the images.

Package repositories are selected automatically. For scheduler, Oracle support, platform, and other build choices, see [Build options](cta-dev.md#build-options). The same options apply when invoking `./ci/cta-dev.sh` directly.

The default platform is selected in `project.json` (currently `el9`). For that platform, RPMs are written to `build/el9/RPM/RPMS/x86_64/`. Stop after `build` if you only need packages.

## Build container images (optional)

Build service images from the RPMs, using the same version and build variant:

```bash
./ci/cta-dev.sh images --cta-version 6-local-test
```

This creates local Podman images for `cta-taped`, `cta-maintd`, `cta-rmcd`, `cta-frontend`, and `cta-tools`, under `localhost/cta/ctageneric/`, tagged `6-local-test` in this example:

```bash
podman images --filter reference='localhost/cta/ctageneric/*:6-local-test'
```

If Minikube or k3s is available, the command also attempts to load the images into it. Otherwise, loading is skipped and the images remain in Podman.

## Push images to a registry

If you need to publish the images, tag and push them to a registry. For example:

```bash
podman login registry.example.org
podman tag localhost/cta/ctageneric/cta-taped:6-local-test registry.example.org/cta/cta-taped:6-local-test
podman push registry.example.org/cta/cta-taped:6-local-test
```

For deployment and configuration, see [Docker Images](../../../ops/deploy-and-configure/deployment/installation/docker-images.md) or [RPM Packages and Services](../../../ops/deploy-and-configure/deployment/installation/rpm-packages.md).

For persistent settings and incremental build recovery, see [cta-dev Reference](cta-dev.md).
