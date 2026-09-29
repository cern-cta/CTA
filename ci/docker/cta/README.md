# CTA container images

Each platform directory contains:

- `build.Dockerfile` and `test.Dockerfile`: platform-specific CI execution images.
- `prod.Dockerfile`: shared stages and targets for CTA service, tools, and debug images built from RPMs.
- `build-service.sh`: package installation and cleanup shared by the image targets.
- `etc/yum.repos.d-{public,internal}/`: package repository definitions used by the builds.

For build requirements and commands, see [Building Images & Packages](../../../docs/content/dev/guides/tools-and-environment/building-images-and-packages.md).

For changes to these files, follow [Container Image Conventions](../../../docs/content/dev/guides/conventions/container-images.md). For CI execution-image updates, follow [CI Maintenance](../../../docs/content/dev/contributing/maintainers/ci-maintenance.md#add-or-update-an-image-dependency).

## Building CTA container images

The CTA container images are multi-stage images. The Dockerfile defines several build targets:

- `cta-taped`
- `cta-rmcd`
- `cta-maintd`
- `cta-frontend`
- `cta-tools`

The build requires a directory containing the CTA RPMs.

### Using the build script (recommended)

The helper script requires Podman and builds all image targets in parallel.

Example:

```bash
./ci/build/build_images.sh \
  --tag dev \
  --rpm-src build/el9/RPM/RPMS/x86_64
```

This creates:

```txt
cta/ctageneric/cta-taped:dev
cta/ctageneric/cta-rmcd:dev
cta/ctageneric/cta-maintd:dev
cta/ctageneric/cta-frontend:dev
cta/ctageneric/cta-tools:dev
```

#### Using internal repositories

To enable internal CERN repositories:

```bash
./ci/build/build_images.sh \
  --tag dev \
  --rpm-src build/el9/RPM/RPMS/x86_64 \
  --enable-internal-repos
```

Local images will not install the Oracle-related RPMs

---

## Building manually

The script is only a wrapper around the container build command. A manual build requires:

1. A directory containing the RPMs.
2. Running the build from the directory containing the Dockerfile.
3. Specifying the RPM directory as a build context.

Example:

```bash
cd ci/docker/cta/el9

podman build \
  -f prod.Dockerfile \
  --build-context package_context=/path/to/RPMS/x86_64 \
  --target cta-taped \
  -t cta/ctageneric/cta-taped:dev \
  .
```

Repeat the build with a different `--target` for the other images:

```bash
--target cta-rmcd
--target cta-maintd
--target cta-frontend
--target cta-tools
```

Example:

```bash
podman build \
  -f prod.Dockerfile \
  --build-context package_context=/path/to/RPMS/x86_64 \
  --target cta-tools \
  -t cta/ctageneric/cta-tools:dev \
  .
```

---

## Notes

- The RPM directory is not copied directly into the final images. A temporary repository is created during the build using `createrepo_c`.
- Each service image is built from the shared `base` stage.
- The `cta-tools` image is intentionally larger because it contains additional client utilities required by CI tests.
- The Dockerfiles use features such as `--mount` and `--build-context`, so use a recent Podman version.
