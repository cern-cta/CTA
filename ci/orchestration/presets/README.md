# Presets

These files are Helm values overlays, grouped by their consumer:

- `infra/`: PostgreSQL instances, connection Secrets, PVCs and optional telemetry dependencies.
- `cta/`: shared CTA service/tools settings, grouped into catalogue, scheduler, authentication, telemetry and test overlays.
- `eos/`: EOS authentication and stress settings.

Use repeatable `--cta-values`, `--eos-values` and `--dcache-values` options.
Defaults are applied first, then supplied files in command-line order.
Supplying `--infra-values` files replaces the default local provisioning selection.
Use `infra/external.yaml` when no local database or scheduler storage is needed.
With `create_instance.sh`, `--catalogue-secret` and `--scheduler-secret` select and copy connection Secrets and omit the corresponding default provisioning.
Explicit infrastructure presets control provisioning independently of these connection options.

Compose the PostgreSQL scheduler preset before its external-connection override.
Likewise, apply common stress settings before PostgreSQL stress tuning.

See the [cta-dev reference](../../../docs/content/dev/guides/tools-and-environment/cta-dev.md) for defaults and deployment examples, including [external database connections](../../../docs/content/dev/guides/tools-and-environment/cta-dev.md#connect-to-an-external-database).
Follow [Helm & Orchestration Conventions](../../../docs/content/dev/guides/conventions/orchestration.md) when changing deployment values.
