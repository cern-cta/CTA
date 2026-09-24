# Example Configuration Files

Below you can find the example config files for the various CTA services.


## CTA Frontend

???+ example "cta-frontend.example.conf"

    ```toml
    --8<--
    frontend/grpc/cta-frontend.example.conf
    --8<--
    ```

## CTA Taped

???+ example "cta-taped.example.conf"

    ```toml
    --8<--
    taped/daemon/cta-taped.example.conf
    --8<--
    ```

## CTA Maintd

???+ example "cta-maintd.example.toml"

    ```toml
    --8<--
    maintd/cta-maintd.example.toml
    --8<--
    ```

## CTA RMCD

???+ example "cta-rmcd.example.conf"

    ```toml
    --8<--
    mediachanger/rmcd/cta-rmcd.example.toml
    --8<--
    ```

## OpenTelemetry Declarative Configuration

See [Enabling OpenTelemetry Metrics](./../../monitoring/metrics.md#enabling-metrics)

???+ example "cta-otel.example.yaml"

    ```toml
    --8<--
    lib/telemetry/cta-otel.example.yaml
    --8<--
    ```
