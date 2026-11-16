!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Health Checks and Alerts

Operator checks that complement [Logging](logging.md) and [Metrics](metrics.md).

## Service and database health

TODO: Document checks for frontends, tape daemons, maintenance routines, catalogue, and scheduler.

## Tape activity and request progress

TODO: Document signals for stalled requests, repeated mount failures, and capacity problems.

## Disk-system health

Document integration health checks and CTA-facing symptoms here; use [Troubleshooting](../../troubleshooting/troubleshooting.md) when investigating failures.

## Health Probing

These options apply to services using the CTA Runtime Library. Check the service option reference for availability.

When running a CTA service, it is useful to understand:

- Are my services ready to do work?
- Are my services alive (not stuck)?

In the Kubernetes world, these issues are addressed using [Health Probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/). However, even outside of Kubernetes, one can add probes to understand the readiness/liveness of each service and take action if necessary.

In the world of health probing, there are a few different types of probes. For simplicity, we concern ourselves only with two:

1. Readiness Probes

    - Is the process temporarily unable or unfit to do work?
    - If yes, keep the process running, but stop feeding it work (if relevant)

2. Liveness Probes

    - Is the process irrecoverably unhealthy (e.g. deadlock)?
    - If yes, restart it

Examples:

- If you notice that the DB connection went down, your readiness probe should fail, but your liveness probe should pass. You don't want to restart the service, because that won't solve the problem and will just cause a tight crash loop
- If a process is hanging and a restart may fix it, that's when your liveness probe should fail.

Services that use the CTA Runtime Library have the option to expose two HTTP endpoints that can be probed for readiness and liveness:

1. `/health/ready`
    - OK: status code `200`
    - Not ready: status code `503`
2. `/health/live`
    - OK: status code `200`
    - Not ready: status code `503`

These health endpoints can be enabled and configured as follows:

???+ "health.example.toml"

    ```toml
    enabled_features = ["health_server"]

    [health_server]
    transport = "tcp"

    [health_server.tcp]
    host = "127.0.0.1"
    port = 8080
    ```

Include `"health_server"` alongside any other enabled features in the root list.
Use `host = "0.0.0.0"` to accept connections on all IPv4 interfaces.
Each service sharing a host needs its own TCP port.

For a Unix socket, select `transport = "unix"`, omit `[health_server.tcp]` and start the service with `--runtime-dir <runtime-dir>`.
The socket is created at `<runtime-dir>/health.sock`.
To disable the health server, remove it from `enabled_features` and omit `[health_server]` and its child sections.


It is recommended to enable the health server on services that support it so that they can be monitored and (if your deployment strategy allows) restarted when necessary.
