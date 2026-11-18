# Pipeline images

These images, along with the `build.Dockerfile` and `test.Dockerfile` under `ci/docker/cta/$PLATFORM` make up the pipeline images used in the GitLab CI.

All Docker images in this directory must be platform agnostic. That is, if the pipeline is run with a different platform (e.g. el10 instead of el9), then any job relying on an image from this directory should not break.

See [Container Image Conventions](../../../docs/content/dev/guides/conventions/container-images.md) and [CI Maintenance](../../../docs/content/dev/contributing/maintainers/ci-maintenance.md) for image updates and adding dependencies.

## Shared images

| Image | Purpose |
| --- | --- |
| `automation` | Pipeline helpers and Python linting, tests, builds, and publication. |
| `rust` | Rust checks, tests, coverage, and documentation builds. |
| `source-checks` | C++ static analysis and formatting, shell, YAML, and secret checks. |
| `repository-publish` | CERN repository publication using Kerberos, XRootD, and DNF. |
| `danger` | Merge-request review automation. |
| `lintian` | Debian package inspection. |
