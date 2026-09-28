# CTA container images

Each platform directory contains:

- `build.Dockerfile` and `test.Dockerfile`: platform-specific CI execution images.
- `prod.Dockerfile`: shared stages and targets for CTA service, tools, and debug images built from RPMs.
- `build-service.sh`: package installation and cleanup shared by the image targets.
- `etc/yum.repos.d-{public,internal}/`: package repository definitions used by the builds.

For build requirements and commands, see [Building Images & Packages](https://cta.docs.cern.ch/latest/dev/guides/tools-and-environment/building-images-and-packages/) ([documentation source](../../../docs/content/dev/guides/tools-and-environment/building-images-and-packages.md)).

For changes to these files, follow [Container Image Conventions](../../../docs/content/dev/guides/conventions/container-images.md). For CI execution-image updates, follow [CI Maintenance](../../../docs/content/dev/contributing/maintainers/ci-maintenance.md#add-or-update-an-image-dependency).
