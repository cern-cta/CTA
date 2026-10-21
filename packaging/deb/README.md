# Debian 13 packages

- amd64; PostgreSQL catalogue and scheduler; no Oracle or objectstore.
- One `cta` package containing services, libraries and tools; automatic `cta-dbgsym` when enabled.
- Test files follow `CTA_BUILD_TEST_PACKAGES`; services are installed disabled and stopped until configured.
- Service environment files use `/etc/default`; site configuration belongs in `/etc/cta`.
- Dependencies are defined in `project.json`. The build image uses the unsigned testing APT repository by default; override `CTA_DEPENDENCY_REPOSITORY` to use stable.

Run from the repository root with submodules checked out:

```sh
podman build -t cta-build-debian13 -f ci/docker/cta/debian13/build.Dockerfile .
podman run --rm -v "$PWD:/source" -w /source cta-build-debian13 \
  sh -c 'cmake -S . -B build/debian13 -DCTA_BUILD_MODE=binary-package -DCTA_VERSION=6-dev -DJOBS_COUNT=4 && cmake --build build/debian13 --target cta_deb'
```

Packages are written to `build/debian13/DEB/`.
Use `cta_dsc` for an unsigned source package, or `CTA_BUILD_MODE=compile` for a normal build.
The existing `ci/build/build_packages.sh` wrapper also supports Debian; select `--scheduler-type pgsched` (Oracle defaults to off).
The existing PostgreSQL test harness requires root inside the build container; use `CTA_RUN_UNIT_TESTS=OFF` for an unprivileged package build.
Rust builds download locked Cargo dependencies and use the same nightly toolchain as EL9.

Binary packaging builds directly from the checkout and reuses `build/debian13/compile/`.
The top-level `debian/` directory is generated and ignored by Git; packaging state is refreshed on each build.
Run one Debian package build at a time per checkout.
`cta-dev build --platform debian13 --clean-build-dirs` removes the compiled output and generated packaging state.
Source packages still use a separate clean staging tree.
