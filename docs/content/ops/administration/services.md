# Service Administration

Operate the core CTA services independently of the disk-system services.

!!! info "Documentation outline"
    The sections below reserve space for the detailed documentation to be added.

## Start, stop, and restart

Document dependency-aware startup, shutdown, and restart procedures. See [RPM Packages and Services](../deployment/installation/rpm-packages.md).

## Health and readiness

Document routine health checks and responses to unhealthy services. See [Health Checks and Alerts](../monitoring/health-and-alerts.md).

## Credentials and access

Document administrator access management and credential rotation. See [Authentication Configuration](../configuration/authentication.md).

## Signal Handling

Services that use the CTA Runtime Library support the following signals:

- `SIGTERM`: gracefully shutdown the service.
- `SIGHUP`: reopen the log file descriptor without reloading configuration. This allows for rename-based log rotation.

    !!! example

        Log rotation can be done by first moving the log file to a different location and then sending `SIGHUP` to the process. Without sending the signal, the service would keep writing to the moved file.
