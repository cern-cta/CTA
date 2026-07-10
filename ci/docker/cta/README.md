# Docker

This directory contains for each platform a set of file. Every platform must contain the following files:

- `build.Dockerfile` dockerfile for creating the build image used to compile CTA in `build_deploy.sh`
- `prod.Dockerfile` dockerfile for creating an image per service based on locally-built RPMs. The images that can be built from this Dockerfile are immutable and are suitable for production.

And for RHEL based:
- `etc/yum.repos.d-internal/*.repo` for the internal yum/dnf repos
- `etc/yum.repos.d-public/*.repo` for the public yum/dnf repos

The Dockerfile naming convention is `<environment>[-<suffix>].Dockerfile`.

# Building CTA container images

The CTA container images are multi-stage images. The Dockerfile defines several build targets:

- `cta-taped`
- `cta-rmcd`
- `cta-maintd`
- `cta-frontend`
- `cta-tools`

The build requires a directory containing the CTA RPMs.

## Using the build script (recommended)

The helper script builds all image targets in parallel.

Example:

```bash
./ci/build/build_image.sh \
  --tag dev \
  --rpm-src build_rpm/RPM/RPMS/x86_64
```

This creates:

```
cta/ctageneric/cta-taped:dev
cta/ctageneric/cta-rmcd:dev
cta/ctageneric/cta-maintd:dev
cta/ctageneric/cta-frontend:dev
cta/ctageneric/cta-tools:dev
```

### Using Docker instead of Podman

```bash
./ci/build/build_image.sh \
  --container-runtime docker \
  --tag dev \
  --rpm-src build_rpm/RPM/RPMS/x86_64
```

### Loading images into the local Kubernetes runtime

For local Kubernetes setups:

```bash
./ci/build/build_image.sh \
  --tag dev \
  --rpm-src build_rpm/RPM/RPMS/x86_64 \
  --load-into-k8s
```

The script automatically detects:

* `minikube`
* `k3s`

and imports the generated images into the corresponding image store.

### Using internal repositories

To enable internal CERN repositories:

```bash
./ci/build/build_image.sh \
  --tag dev \
  --rpm-src build_rpm/RPM/RPMS/x86_64 \
  --enable-internal-repos
```

Local images will not install the Oracle-related RPMs

---

# Building manually

The script is only a wrapper around the container build command. A manual build requires:

1. A directory containing the RPMs.
2. Running the build from the directory containing the Dockerfile.
3. Specifying the RPM directory as a build context.

Example:

```bash
cd ci/docker/cta/el9

podman build \
  -f prod.Dockerfile \
  --build-context rpm_context=/path/to/RPMS/x86_64 \
  --target cta-taped \
  -t cta/ctageneric/cta-taped:dev \
  .
```

The same command works with Docker:

```bash
docker build \
  -f prod.Dockerfile \
  --build-context rpm_context=/path/to/RPMS/x86_64 \
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
  --build-context rpm_context=/path/to/RPMS/x86_64 \
  --target cta-tools \
  -t cta/ctageneric/cta-tools:dev \
  .
```

---

# Notes

* The RPM directory is not copied directly into the final images. A temporary repository is created during the build using `createrepo_c`.
* Each service image is built from the shared `base` stage.
* The `cta-tools` image is intentionally larger because it contains additional client utilities required by CI tests.
* BuildKit features are required (`--mount`, `--build-context`), so use recent Docker or Podman versions.
