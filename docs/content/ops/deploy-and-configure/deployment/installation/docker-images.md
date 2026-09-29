!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Docker Images

CTA publishes Docker images. This page is the operator entry point for deploying those images.

TODO: Image locations, tag selection, and deployment examples will be documented here.

To build your own packages or service images from source, follow [Building Images & Packages](../../../../dev/guides/tools-and-environment/building-images-and-packages.md).

## Select an image

TODO: Document the published registry, service images, release tags, architectures, and selecting a fixed image version or digest.

## Configure and run services

TODO: Document entrypoints, configuration and credential mounts, networking, database connectivity, and service identities. Keep shared settings in [Service Runtime](../../configuration/service-runtime.md).

## Storage and hardware access

TODO: Document persistent and temporary storage, writable runtime directories, permissions, and device access for tape services. Installing packages inside an image does not provide the host's systemd directory management.

## Logging, health, and shutdown

TODO: Document log collection, supported health endpoints, signal delivery, and graceful termination. Link to [Logging](../../../run-and-maintain/monitoring/logging.md), [Health Checks](../../../run-and-maintain/monitoring/health-and-alerts.md), and [Service Administration](../../../run-and-maintain/administration/services.md).

## Commissioning

Follow [Initialisation and Verification](../initialisation.md) after configuring the services and disk-system integration.
