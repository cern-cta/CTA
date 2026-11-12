!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Docker Images

CTA publishes Docker images. This page is the operator entry point for deploying those images.

TODO: Image locations, tag selection, and deployment examples will be documented here.

To build your own packages or service images from source, follow [Building Images & Packages](../../../../dev/guides/tools-and-environment/building-images-and-packages.md).

## Select an image

TODO: Document the published registry, service images, release tags, architectures, and selecting a fixed image version or digest.

## Configure and run services

TODO: Document entrypoints, configuration and credential mounts, networking, and database connectivity. Keep shared settings in [Service Runtime](../../configuration/service-runtime.md).

The EL9 and EL10 service images use UID and GID 1000 for `cta`.
Supplementary groups are assigned when containers are launched.
The Helm workloads set `runAsUser`, `runAsGroup`, and `fsGroup` to 1000; only the taped and rmcd workloads set `supplementalGroups`, defaulting to `[33]`.
Set `pod.supplementalGroups` in each chart to match the host device group IDs.
For the umbrella CTA chart, override both services in your values file, for example:

```yaml
taped:
  pod:
    supplementalGroups: [6]
rmcd:
  pod:
    supplementalGroups: [6]
```

Deploy images with matching charts; when building from source, rebuild the images before deploying the updated charts.

## Storage and hardware access

TODO: Document persistent and temporary storage, writable runtime directories, permissions, and device access for tape services. Installing packages inside an image does not provide the host's systemd directory management.

When upgrading from images that use `tape` as the primary group, check existing writable volumes for ownership and permissions compatible with `cta:cta`.
For volumes whose ownership is not managed by Kubernetes, migrate CTA-owned runtime and log files while the affected workloads are stopped.
Keep host tape devices in the `tape` group and ensure their numeric group ID matches the container's supplementary group.
For standalone Docker or Podman containers running `cta-taped` or `cta-rmcd`, pass `--group-add 33` when the device group ID is 33; otherwise use the device's actual group ID.
Device mappings and any required capabilities must also be configured.

## Logging, health, and shutdown

TODO: Document log collection, supported health endpoints, signal delivery, and graceful termination. Link to [Logging](../../../run-and-maintain/monitoring/logging.md), [Health Checks](../../../run-and-maintain/monitoring/health-and-alerts.md), and [Service Administration](../../../run-and-maintain/administration/services.md).

## Commissioning

Follow [Initialisation and Verification](../initialisation.md) after configuring the services and disk-system integration.
