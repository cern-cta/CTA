# Pipeline images

These images, along with the `build.Dockerfile` and `test.Dockerfile` under `ci/docker/cta/$PLATFORM` make up the pipeline images used in the GitLab CI.

All Docker images in this directory must be platform agnostic. That is, if the pipeline is run with a different platform (e.g. el10 instead of el9), then any job relying on an image from this directory should not break.

See [Container Image Conventions](../../../docs/content/dev/guides/conventions/container-images.md) and [CI Maintenance](../../../docs/content/dev/contributing/maintainers/ci-maintenance.md) for image updates and adding dependencies.
