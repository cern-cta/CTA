---
title: Deployment recommendations
---

# Recommendations & Tips

Below are a few recommendations and best-practices for deploying CTA.

## Separate Frontends for Workflow Events and Admin Commands

Since version `5.11.19` the CTA frontend has to be run as two separate services:

 * the **Workflow Engine (WFE) Frontend**; and
 * the **Admin API**.

They both run the same software, only with different configuration files. This allows for greater flexibility in the software and
authentication architecture and prevents the load introduced by admin commands from interfering with the processing of workflow events.

It is advised to run at least 2 Admin API Frontend endpoints:

- one as an endpoint for **operators, operations monitoring systems and tape servers** with configured `cta-admin` CLI;
- another one that serves as **test/debug process**: used to debug crashing admin commands, deploy and test the next version of the Admin
API Frontend with no impact on production operations or monitoring.

### Isolate User requests from Repack by separating scheduler backends

Repack objectstore scheduler can put a lot of pressure on the objectstore scheduler. This can lead to a significant performance degradation on user requests if repack and user share the same objectstore backend.
Separating User and Repack sheduler backends allows to completely isolate these disjoint use cases and preserve end user service performance during intense tape repack campaigns.

Separating User and Repack requires:

- separate resources for another scheduler backend for repack
- 1 dedicated Admin API (`cta-frontend-admin`) endpoint to allow operators to submit repack requests
- moving some tape drives from user scheduler to repack scheduler when repack is needed

!!! tip

    Use the new `REPACK` archive route to create tape pools for the REPACK VO. This feature prevents consuming all experiment writeable tapes when repack to avoid *user write starvation*. It also avoid mixing old repacked files with newly archived user files.

## CTA Services without Hardware Constraints

While `cta-taped` and `cta-rmcd` need to run on servers that have access to the tape hardware, the frontend/API services and `cta-maintd`
have no such constraints and can be run anywhere.

## JSON Logging

Every CTA service except for `cta-rmcd` supports JSON logs. It is **highly recommended to configure CTA to produce JSON logs** as these should be natively processed by modern monitoring tools.

## Strict Config Checking

!!! info

    At the time of writing, only `cta-maintd` supports this. The plan is to roll out these changes to the other services as well, see [this GitLab Epic](http://gitlab.cern.ch/groups/cta/-/epics/32).

When defining a config file for a service, there are multiple problems that can occur:

1. An operator can make a typo in the configuration option, causing them to think something is configured, but in reality the option is ignored.
2. Software may change option names between (major) releases, causing previously working options to break (transparently).
3. Software may change default values of config options, causing a change in behaviour despite the operator never changing the config file.

Services that use the CTA Runtime Library support the `--config-strict` flag which can prevent all of these. Concretely it enforces the following rules:

- Every provided config value MUST be a valid config entry. That means it will fail when entries are present that are not consumed by CTA. This mitigates (1) and (2).
- Every single config value parsed by CTA MUST be present in the config file. As a result, changes in default values will not affect the deployment, because every value is explicit and defaults are no longer relied upon. This mitigates (2) and (3).

It is possible to check the validity of a config beforehand using the `--config-check` flag, which can be combined with the `--config-strict` flag. This will simply the config and exit if it deems it successful.

It is highly recommended to use `--config-strict` when deploying CTA to ensure operators have full control and transparency over the consumed config values.

## Health Probing

!!! info

    At the time of writing, only `cta-maintd` supports this. The plan is to roll out these changes to the other services as well, see [this GitLab Epic](http://gitlab.cern.ch/groups/cta/-/epics/32).

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

## Runtime Directory

!!! warning "CTA 6 and later"

    The consistent runtime-directory behavior described in this section applies to CTA 6 and later. In CTA 5, only `cta-maintd` supports it.

Services that use the CTA Runtime Library have the option to populate an existing directory with runtime metadata.:

- **`config.toml`**: the main config file of the service. This is the same file as passed in the `--config <path>` flag.
- **`cta-logging.schema.json`**: a JSON schema for the logs. See [Logging](./../monitoring/logging.md).
- **`catalogue.config_file`**: the file containing the catalogue connection string. Note that the name of the file reflects the full TOML path to ensure this can be correlated with the corresponding option in the config file.
- **`scheduler.config_file`**: the file containing the scheduler connection string. Its name reflects the full TOML path of the option. Only exists when CTA is compiled with the Postgres scheduler.
- **`telemetry.config_file`**: the file containing the declarative telemetry configuration. Its name reflects the full TOML path of the option.
- **`version.json`**: a simple JSON file containing the name of the service and its version. Example:

    ```json
    {"service": "cta-maintd", "version": "5.11.18.0"}
    ```

The runtime directory provides a snapshot of all files consumed by the service when it started. Typically, changing the config file on disk will not automatically restart a service, which means that your service may be running with a different config file than what you may expect. As such, the runtime directory allows operators to detect inconsistencies between the intended configuration file (e.g. the config path used in `--config <path>`) and the files actually consumed by the service (e.g. `<runtimedir>/config.toml`).

To enable this feature, use the `--runtime-dir <path>` flag. The lifecycle of the directory itself is not managed by the CTA process. The directory must exist before the process starts and must be readable and writable by the service. The process does not clean it up. The deployment layer manages it as follows:

- **Systemd RPM deployment**: The packaged unit uses `RuntimeDirectory=` to manage the directory lifecycle. When running multiple processes on the same host, ensure the directory name is unique per unit. See [RPM Packages and Services](rpm-packages-and-services.md#log-and-runtime-directories).
- **Kubernetes**: Simply create a directory as an `emptyDir` volume. Because the lifetime of the service is tied to the lifetime of the pod, there should be no issues with name clashes.

## Signal Handling

!!! warning "CTA 6 and later"

    The consistent signal behavior described in this section applies to CTA 6 and later. In CTA 5, only `cta-maintd` supports these runtime-library signals.

Services that use the CTA Runtime Library support the following signals:

- `SIGTERM`: gracefully shutdown the service.
- `SIGHUP`: reopen the log file descriptor without reloading configuration. This allows for rename-based log rotation.

    !!! example

        Log rotation can be done by first moving the log file to a different location and then sending `SIGHUP` to the process. Without sending the signal, the service would keep writing to the moved file.
