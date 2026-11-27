# Presets

These files are Helm values overlays, grouped by their consumer:

- `infra/`: PostgreSQL instances, connection Secrets, PVCs and optional telemetry dependencies.
- `cta/`: shared CTA service/tools settings, grouped into catalogue, scheduler, authentication, telemetry and test overlays.
- `eos/`: EOS authentication and stress settings.

See the [cta-dev deployment options](../../../docs/content/dev/guides/tools-and-environment/cta-dev.md#deployment-options) for defaults, override ordering and external connections.
Follow [Helm & Orchestration Conventions](../../../docs/content/dev/guides/conventions/orchestration.md) when changing deployment values.
