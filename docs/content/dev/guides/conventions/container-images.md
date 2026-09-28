# Container Image Conventions

Follow these conventions when changing CTA service, tooling, or CI execution images. For build commands, see [Building Images & Packages](../tools-and-environment/building-images-and-packages.md).

## Image layout and purpose

- Keep platform-specific Dockerfiles under `ci/docker/cta/<platform>/`. `prod.Dockerfile` provides service and tools/debug targets; `build.Dockerfile` and `test.Dockerfile` provide CI execution environments.
- Keep shared CI execution images under `ci/docker/pipeline/` platform-independent.
- Dockerfiles MUST be named `[<purpose>.]Dockerfile`. Use `Dockerfile` when no purpose prefix is needed.
- Prefer shared stages and small helper scripts for related image variants. Use separate Dockerfiles when the images have distinct responsibilities; conditional build options are acceptable when they avoid duplication.
- Use multi-stage builds where they keep build-only tools and inputs out of final images. Keep service images focused on runtime requirements; tools and debug images may include additional utilities for their intended tasks.

## Dependencies and build behaviour

- Choose maintained base images from official sources, using a minimal variant where it meets the image's needs.
- Install stable tools and system dependencies at image build time. Follow the [CI dependency installation guidance](gitlab.md#execution-images-and-scripts) for justified runtime installations.
- Distinguish image rebuilds from image consumption. CTA service-image builds intentionally allow base-image and OS-package updates on rebuild; preserve the declared CTA dependency constraints and document intentional floating dependencies beside the Dockerfile.
- Consume published images through explicit versioned tags or digests rather than a moving `latest` tag. CI execution images use the centrally selected image version; follow [Add or update an image dependency](../../contributing/maintainers/ci-maintenance.md#add-or-update-an-image-dependency) to build, validate, and adopt changes.
- Group related package installation and cleanup in the same layer. Use cache mounts where appropriate, and keep build-only files and package caches out of the final image.
- Place frequently changing inputs after stable setup where practical to preserve build-cache reuse. Ensure changes to mounted build inputs invalidate the relevant build step.
- Extract complex installation logic into helper scripts and explain non-obvious build choices beside the implementation.

## Runtime behaviour

- Images MAY define sensible defaults for users, executable paths, and logging. Deployment-specific configuration MUST be supplied at runtime through supported configuration files, environment variables, or secret mounts.
- Service images MUST run the daemon in the foreground and log to standard output/error by default. Keep startup commands simple and ensure termination signals reach the daemon.
- Images SHOULD run as a non-root user unless their purpose requires root. Keep service user and group IDs consistent with the deployment's volume-ownership requirements.
- Service images MUST support the runtime-library `--runtime-dir` interface and allow the daemon to write to the configured directory. Installing an RPM does not run systemd's directory-management directives inside a container.
- Define volumes, writable-directory ownership, device access, and deployment security settings in the manifests; see [Helm & Orchestration Conventions](orchestration.md). Deployment-specific file logging and retention also belong there.

## Secrets and publication

- Secrets MUST NOT be embedded in Dockerfiles, image configuration, copied files, or image layers. Removing a secret in a later layer does not remove it from the image history.
- Use build-time secret mounts or the build tool's authentication mechanism when credentials are required during a build; do not pass credentials through ordinary build arguments or environment defaults.
- Images published to public registries MUST NOT contain Oracle-related RPMs, including OCCI packages. Keep Oracle-enabled variants in approved private destinations.
- Preserve the CI publication check in `.gitlab/ci/build-image.gitlab-ci.yml`, which checks for Oracle/OCCI packages before pushing images outside the approved destination prefixes. Do not bypass it when adding new image targets or publication paths.
