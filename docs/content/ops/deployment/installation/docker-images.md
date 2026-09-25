# Docker Images

CTA publishes Docker images. This page is the operator entry point for deploying those images.

!!! info "Documentation outline"
    Image locations, tag selection, and deployment examples will be documented here.

## Select an image

Document the published registry, service images, release tags, architectures, and selecting a fixed image version or digest.

## Configure and run services

Document entrypoints, configuration and credential mounts, networking, database connectivity, and service identities. Keep shared settings in [Service Runtime](../../configuration/service-runtime.md).

## Storage and hardware access

Document persistent and temporary storage, writable runtime directories, permissions, and device access for tape services. Installing packages inside an image does not provide the host's systemd directory management.

## Logging, health, and shutdown

Document log collection, supported health endpoints, signal delivery, and graceful termination. Link to [Logging](../../monitoring/logging.md), [Health Checks](../../monitoring/health-and-alerts.md), and [Service Administration](../../administration/services.md).

## Commissioning

Follow [Initialisation and Verification](../initialisation.md) after configuring the services and disk-system integration.
