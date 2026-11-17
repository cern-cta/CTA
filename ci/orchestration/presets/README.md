# Presets

These files are Helm values overlays, grouped by their consumer:

- `infra/`: PostgreSQL instances, connection Secrets, PVCs and optional telemetry dependencies.
- `cta/`: shared CTA service/tools settings, grouped into catalogue, scheduler, authentication, telemetry and test overlays.
- `eos/`: EOS authentication and stress settings.

Use repeatable `--cta-values`, `--eos-values` and `--dcache-values` options.
Defaults are applied first, then supplied files in command-line order.
Supplying `--infra-values` files replaces the default local provisioning selection.
Use `infra/external.yaml` when no local database or scheduler storage is needed.

Compose the PostgreSQL scheduler preset before its external-connection override.
Likewise, apply common stress settings before PostgreSQL stress tuning.

See the [cta-dev reference](../../../docs/content/dev/guides/tools-and-environment/cta-dev.md) for defaults and deployment examples.
Follow [Helm & Orchestration Conventions](../../../docs/content/dev/guides/conventions/orchestration.md) when changing deployment values.
