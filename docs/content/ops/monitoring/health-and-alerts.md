# Health Checks and Alerts

Operator checks that complement [Logging](logging.md) and [Metrics](metrics.md).

!!! info "Documentation outline"
    The sections below reserve space for the detailed documentation to be added.

## Service and database health

Document checks for frontends, tape daemons, maintenance routines, catalogue, and scheduler.

## Tape activity and request progress

Document signals for stalled requests, repeated mount failures, and capacity problems.

## Disk-system health

Keep disk-system-specific checks under its integration guide and document the CTA-facing symptoms here.

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
    # Configuration for a health server. The health server, if enabled, will expose the following endpoints:
    # - host:port/health/ready
    # - host:port/health/live
    # This allows for configuring readiness and liveness probes if so desired.
    [health_server]
      # Whether to enable the health server or not.
      # If omitted, false is used.
      enabled = false
      # Where to accept connections from. Setting this to 127.0.0.1 will make it only accept connections from localhost.
      # Set to 0.0.0.0 to accept connections from anywhere.
      # When use_unix_domain_socket is enabled, this host value will be unused.
      # If omitted, 127.0.0.1 is used.
      host = "127.0.0.1"
      # What port to expose the health server on. Note that multiple processes running on the same host cannot share the same port.
      # When use_unix_domain_socket is enabled, this port value will be unused.
      # If omitted, 8080 is used.
      port = 8080
      # If enabled, expose the health server over a unix domain socket instead. In this case, the values for host and port are ignored.
      # Enabling this will put the corresponding socket file in <runtime-dir>/health.sock.
      # Note that if use_unix_domain_socket is enabled, the service  MUST be started --runtime-dir <runtime-dir> flag.
      # If omitted, false is used.
      use_unix_domain_socket = false
    ```

It is recommended to enable the health server on services that support it so that they can be monitored and (if your deployment strategy allows) restarted when necessary.
