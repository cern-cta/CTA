# Presets

This directory contains a number of `values.yaml` files that can be passed to the Helm deployment for local development purposes.

The files should follow the naming convention:

```txt
<release>-<component>[-<type>]?-values.yaml
```

Presets request named PostgreSQL instances, connection-file Secrets and PVCs from `ci-infra`.
CTA services consume the resulting connection Secrets.
The `dev-telemetry-infra-values.yaml` preset enables and configures the optional Prometheus and collector dependencies.
Scheduler presets also configure the corresponding maintenance routines through `maintd.routines`.

See the [cta-dev reference](../../../docs/content/dev/guides/tools-and-environment/cta-dev.md) for selecting and overriding deployment values.

Follow [Helm & Orchestration Conventions](../../../docs/content/dev/guides/conventions/orchestration.md) when changing deployment values.
