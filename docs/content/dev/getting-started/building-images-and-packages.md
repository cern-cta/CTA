# Building Images and Packages

This page is for those who only want to build the CTA container images locally. For development workflows and a full
description of `cta-dev`, see the [`cta-dev` guide](development-workflow.md).

Building the images requires a Linux machine with [Podman](https://podman.io/) installed. Docker is not supported.

Start by cloning the repo:

```bash
git clone ssh://git@gitlab.cern.ch:7999/cta/CTA.git
cd CTA
git submodule update --init --recursive
```

Build the RPMs first:

```bash
cd ci
./cta-dev.sh build
```

This creates a build container and writes the RPMs to `build_rpm/RPM/RPMS/x86_64` in the repository root.

Build the service images from those RPMs:

```bash
./cta-dev.sh images
```

The default image tag is `5-dev`. To use a distinct version/tag, pass the same values to both stages, e.g.:

```bash
./cta-dev.sh build  --cta-version local-test
./cta-dev.sh images --cta-version local-test
```

This produces images such as `cta/ctageneric/cta-taped:local-test`. The version accepts letters, numbers, dots, and hyphens. See
[Custom versions and image tags](development-workflow.md#custom-versions-and-image-tags) for the corresponding `cta-dev` behavior.

By default, CTA is built for the objectstore scheduler and without Oracle catalogue support. Select the PostgreSQL
scheduler during the RPM build with `--scheduler-type pgsched`. Oracle support affects both the RPMs and image contents,
so pass `--enable-oracle-support` to both `build` and `images` when it is required.

Use `./cta-dev.sh build --help` and `./cta-dev.sh images --help` for the available options. The fuller
[`cta-dev` guide](development-workflow.md) also explains container-runtime selection, public versus CERN-internal repositories, and
incremental build-state handling.
