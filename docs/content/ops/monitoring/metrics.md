---
title: Monitoring metrics
---

# CTA Metrics

CTA can publish metrics in the [OTLP](https://opentelemetry.io/docs/specs/otlp/) format. **This feature is disabled by default** and has to be explicitly configured.
This page describes which metrics CTA can publish, the prerequisites, and how to enable metrics.

Telemetry data is primarily useful for monitoring system health, usage and saturation.
It is typically not intended for long-term archival or audit purposes.

- **Retention**: Keep metrics retention short (for example, 1-2 weeks). This minimizes storage overhead while retaining enough history for operational analysis.
- **Resource attributes**: Many queries and dashboards rely on resource context (such as `service.name`, `host.name`, or `process.title`).
  Configure your metrics backend or OpenTelemetry Collector to attach resource attributes to every metric, or enable automatic resource-to-metric label conversion if your backend supports it.

## Supported Metrics

!!! warning

    Note that CTA Metrics is in its early stages and considered experimental. Published metrics and their attributes are likely to change.

Metric and attribute names follow [OpenTelemetry semantic conventions](https://opentelemetry.io/docs/specs/semconv/), with CTA-specific prefixes for internal domains (e.g., `cta.taped`, `cta.scheduler`).

Resource attributes describe the entity that produced the telemetry (service/process/host). They are not automatically attached to every metric by all backends.

- In Prometheus, resource attributes are exposed as a separate time series named `target_info` (and related info series). They are not labels on each metric by default.
- If you need resource attributes as labels on every series, either:
  1. enable resource -> metric label conversion in your OpenTelemetry Collector pipeline, or
  2. join metrics with `target_info` in PromQL (e.g., `on(...) group_left(...)`) at query time.
- Prometheus label keys are sanitized to be valid identifiers (e.g., `service.instance.id` -> `service_instance_id`).

### Metrics

| Metric Name                             | Type          | Unit | Description                                                                                 | Attributes                                                          |
| --------------------------------------- | ------------- | ---- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| `db.client.connection.count`            | UpDownCounter | 1    | The number of connections that are currently in a state described by the `state` attribute. | `db.namespace` <br> `db.system.name` <br> `state`                   |
| `db.client.operation.duration`          | Histogram     | ms   | Duration of database client operations.                                                     | `db.namespace` <br> `db.system.name` <br> (`error.type`)            |
| `cta.frontend.request.duration`         | Histogram     | ms   | Duration the frontend takes to process a request.                                           | `event.name` <br> `cta.frontend.requester.name` <br> (`error.type`) |
| `cta.frontend.active_requests`          | UpDownCounter | ms   | Number of in-flight frontend requests.                                                      | `event.name` <br> `cta.frontend.requester.name`                     |
| `cta.scheduler.operation.duration`      | Histogram     | ms   | Duration of a CTA scheduling operation.                                                     | `cta.scheduler.operation.name`                                      |
| `cta.scheduler.disk.report.count`       | UpDownCounter | 1    | Number of files reported to disk operation.                                                 | `cta.transfer.direction` <br> (`error.type`)                        |
| `cta.scheduler.repack.report.count`     | UpDownCounter | 1    | Number of repack files reported operation.                                                  | `cta.repack.report.type`                                            |
| `cta.scheduler.repack.expand.count`     | UpDownCounter | 1    | Number of file repack requests expanded operation.                                          | `cta.scheduler.operation.name`                                      |
| `cta.scheduler.operation.duration`      | Histogram     | ms   | Duration of a CTA scheduling operation.                                                     | `cta.scheduler.operation.name`                                      |
| `cta.objectstore.lock.acquire.duration` | Histogram     | ms   | Duration taken to acquire an objectstore lock.                                              | `lock.type`                                                         |
| `cta.objectstore.gc.agent.count`        | UpDownCounter | 1    | Number of garbage collected agents.                                                         |                                                                     |
| `cta.objectstore.gc.object.count`       | UpDownCounter | 1    | Number of garbage collected objects as a result of agent cleanup.                           |                                                                     |
| `cta.objectstore.cleanup.queue.count`   | UpDownCounter | 1    | Number of queues cleaned up.                                                                |                                                                     |
| `cta.objectstore.cleanup.file.count`    | UpDownCounter | 1    | Number of files moved as a result of queue cleanup.                                         |                                                                     |
| `cta.taped.transfer.file.count`         | Counter       | 1    | Number of files transferred using the io medium in the given io direction.                  | `cta.io.direction` <br> `cta.io.medium` <br> (`error.type`)         |
| `cta.taped.transfer.file.size`          | Counter       | by   | Bytes transferred using the io medium in the given io direction.                            | `cta.io.direction` <br> `cta.io.medium`                             |
| `cta.taped.transfer.active`             | UpDownCounter | 1    | Number of threads actively transferring using the io medium in the given io direction.      | `cta.io.direction` <br> `cta.io.medium`                             |
| `cta.taped.buffer.usage`                | Gauge         | by   | Bytes in use by the memory buffer in cta-taped.                                             |                                                                     |
| `cta.taped.buffer.limit`                | Gauge         | by   | Total bytes available for the memory buffer in cta-taped.                                   |                                                                     |
| `cta.taped.mount.duration`              | Histogram     | s    | Duration to mount a tape.                                                                   | `cta.io.direction`                                                  |
| `cta.taped.mount.type`                  | UpDownCounter | 1    | Number of drive sessions with the given mount type.                                         | `cta.taped.mount.type`                                              |
| `cta.taped.drive.status`                | UpDownCounter | 1    | Number of drives in a given state.                                                          | `cta.taped.drive.state`                                             |
| `cta.maintd.routine.duration`           | Histogram     | ms   | Duration to execute a routine of the given type.                                            | `cta.routine.name`                                                  |

### Resource Attributes

| Attribute Name              | Description                                                                                                      |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `service.namespace`         | Logical namespace of the service emitting the metric. Equivalent to the instance name in CTA.                    |
| `service.name`              | Name of the service emitting the metric (e.g. `cta.taped`, `cta.frontend`).                                      |
| `service.version`           | Version of the service emitting the metric.                                                                      |
| `service.instance.id`       | Unique identifier for the specific service instance. Useful when multiple replicas run under the same namespace. |
| `process.title`             | Title of the process within the service. For `cta.taped`, this means per-drive.                                  |
| `host.name`                 | Host on which the service is running.                                                                            |
| `cta.scheduler.namespace`   | Logical name of the scheduler backend in use (e.g. `disk`, `tape`).                                              |
| `tape.drive.name`           | Name of the tape drive (only exposed for `cta-taped`).                                                           |
| `tape.library.logical.name` | Name of the logical library of the tape drive (only exposed for `cta-taped`).                                    |

### Metric Attributes

| Attribute Name                 | Description                                                                                            |
| ------------------------------ | ------------------------------------------------------------------------------------------------------ |
| `cta.scheduler.operation.name` | Name of the CTA scheduling operation (e.g. `enqueueArchive`, `cancelRepack`).                          |
| `cta.repack.report.type`       | Type of repack report (e.g. `ArchiveSuccess`, `ArchiveFailed`, `RetrieveSuccess`, `kRetrieveFailed`).  |
| `cta.frontend.requester.name`  | Name of the frontend event requester (e.g. user, subsystem, or service calling the API).               |
| `cta.transfer.direction`       | Direction of the transfer (`archive` or `retrieve`).                                                   |
| `cta.io.direction`             | Direction of the io (`read` or `write`). Essentially a lower-level version of `cta.transfer.direction` |
| `cta.io.medium`                | Medium used for io (`disk` or `tape`).                                                                 |
| `cta.taped.drive.state`        | State that the drive is in                                                                             |
| `cta.taped.mount.type`         | Type of mount.                                                                                         |
| `cta.maintd.routine.name`      | Name of the routine executed by maintd.                                                                |
| `db.namespace`                 | Database namespace (schema or logical grouping).                                                       |
| `db.system.name`               | Name of the database system (e.g., `postgresql`, `oracle`).                                            |
| `event.name`                   | Name of the event being tracked (e.g., frontend or scheduler event).                                   |
| `error.type`                   | Classification of an error that occurred (e.g., `network`, `timeout`, `permission_denied`).            |
| `le`                           | Histogram bucket upper bound (“less than or equal” duration in ms).                                    |
| `lock.type`                    | Type of lock being acquired in the object store or internal resource (e.g., `read`, `write`).          |
| `state`                        | Operational or lifecycle state represented by the metric (e.g., `active`, `queued`, `failed`).         |

---

## Prerequisites

In order to ingest OTLP metrics, an endpoint must be available for CTA to push metrics to.
In practice, this means you will need to deploy your own [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/).

The collector can then export metrics to a time-series database such as Prometheus or Grafana Mimir, where they can be visualized in Grafana or other compatible dashboards.

> Setting up and managing this infrastructure is outside the scope of this documentation.
> Refer to the respective documentation for installation and configuration guidance:
>
> - [OpenTelemetry Collector documentation](https://opentelemetry.io/docs/collector/)
> - [Prometheus setup guide](https://prometheus.io/docs/introduction/overview/)
> - [Grafana Mimir setup](https://grafana.com/docs/mimir/latest/)

## Enabling Metrics

CTA services expose metrics publishing as a configurable option.
To enable OTLP metrics, set the appropriate configuration parameter in each service’s configuration file.

At the time of writing, three services support publishing metrics:

- `cta-maintd`
- `cta-taped`
- `cta-frontend`

!!! info

    At the time of writing, all three of these have different configuration formats, which makes telemetry tedious to configure. Only `cta-maintd` currently supports the target configuration format (both for the service itself and for telemetry). The migration of the config formats for the frontend and taped will happen in the next few months, see [this GitLab Epic](http://gitlab.cern.ch/groups/cta/-/epics/32).

Once metrics publishing is enabled and the OpenTelemetry Collector is receiving data:

1. Confirm that CTA is not showing any error logs related to OpenTelemetry.
2. Confirm that your collector logs show incoming OTLP data from CTA services.
3. Query the metrics backend (e.g., Prometheus) for metrics that CTA publishes.
4. Use Grafana to visualize the metrics.


!!! example

    A minimal verification query in Prometheus might look like:

    ```promql
    db_client_operation_duration_count
    ```

### `cta-maintd`

The maintenance daemon uses the OpenTelemetry declarative configuration format. This provides superior flexibility and robustness in comparison to implementing every option in the configuration of the service itself. To enable telemetry, set the following options:

```toml
[telemetry]
  # Path to the declarative config file to initialise the OpenTelemetry SDK.
  # If omitted or an empty string, telemetry will be disabled.
  config_file = "/etc/cta/cta-otel.yaml"

[experimental]
  # false by default
  telemetry_enabled = true
```

Then all configuration of OpenTelemetry is done through `/etc/cta/cta-otel.yaml`. There are various examples online of how to configure as it, but it is recommended to follow the bundled example file.

???+ example "cta-otel.example.yaml"

    ```toml
    --8<--
    lib/telemetry/cta-otel.example.yaml
    --8<--
    ```

### `cta-taped`

!!! warning

    `cta-taped` does not support the declarative OpenTelemetry configuration yet. As such, it must be configured directly through its config file. Before telemetry is taken out of experimental, this will be updated.

To enable OTLP metrics publishing in `cta-taped`, set the following options in its configuration file:

???+ "cta-taped.example.conf"

    ```ini
    # Telemetry is an experimental feature and must be explicitly enabled before it can be used.
    # Note that this flag alone is not sufficient to start producing telemetry.
    experimental telemetryEnabled true

    # Used to control cardinality.
    # If set to false, each restart of a process will generate a new unique ID for the `service.instance.id`.
    # If set to true, `service.instance.id` remains constant across restarts.
    # As cta-taped forks, setting this to false will result in high cardinality metrics,
    # due to drive sessions starting new processes.
    telemetry retainInstanceIdOnRestart true

    # Metrics backend to use. Possible options are NOOP, OTLP_HTTP, OTLP_GRPC, STDOUT, FILE
    # Default is NOOP, meaning no metrics are collected/exported
    telemetry metricsBackend OTLP_GRPC

    # Amount of time in milliseconds between exports
    telemetry metricsExportInterval 15000

    # Timeout for a single export
    telemetry metricsExportTimeout 3000

    # Service location of the OTLP collector in case the OTLP backend is used
    telemetry metricsOtlpEndpoint endpoint:port

    # Basic authentication: configured both username and passwordFile Adds the header "authorization: Basic <base64(username:password)>"
    # Username to use for setting up basic auth for push metrics over HTTP
    telemetry metricsOtlpAuthBasicUsername username
    # File location containing the password (not base64 encoded) to set up basic auth for push metrics over HTTP
    telemetry metricsOtlpAuthBasicPasswordFile /path/to/password/file
    ```

After updating the configuration, restart cta-taped for the changes to take effect.

### `cta-frontend`

!!! warning

    `cta-frontend` does not support the declarative OpenTelemetry configuration yet. As such, it must be configured directly through its config file. Before telemetry is taken out of experimental, this will be updated.

cta-frontend supports similar metrics publishing, but its configuration names are slightly different.

???+ "cta-frontend.example.conf"

    ```ini
    # Telemetry is an experimental feature and must be explicitly enabled before it can be used.
    # Note that this flag alone is not sufficient to start producing telemetry.
    cta.experimental.telemetry.enabled true

    # Used to control cardinality.
    # If set to false, each restart of a process will generate a new unique ID for the `service.instance.id`.
    # If set to true, `service.instance.id` remains constant across restarts.
    # The cta-frontend should not restart frequently, so this option can be left off if desired.
    cta.telemetry.retain_instance_id_on_restart false

    # Metrics backend to use. Possible options are NOOP, OTLP_HTTP, OTLP_GRPC, STDOUT, FILE
    # Default is NOOP, meaning no metrics are collected/exported
    cta.telemetry.metrics.backend OTLP_GRPC

    # Amount of time in milliseconds between exports
    cta.telemetry.metrics.export.interval 15000

    # timeout for a single export
    cta.telemetry.metrics.export.timeout 3000

    # Service location of the OTLP collector in case the OTLP backend is used
    cta.telemetry.metrics.otlp.endpoint endpoint:port

    # Basic authentication: configured both username and passwordFile Adds the header "authorization: Basic <base64(username:password)>"
    # Username to use for setting up basic auth for push metrics over HTTP
    cta.telemetry.metrics.otlp.auth.basic.username username
    # File location containing the password (not base64 encoded) to set up basic auth for push metrics over HTTP
    cta.telemetry.metrics.otlp.auth.basic.password_file /path/to/password/file
    ```

Restart cta-frontend after applying the changes.
If metrics are correctly enabled, you should begin to see metric data arriving at your collector within a few seconds.

---
