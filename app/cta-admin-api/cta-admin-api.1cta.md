---
date: 2026-02-18
section: 1
title: CTA-ADMIN-API
header: The CERN Tape Archive (CTA)
---

# NAME

cta-admin-api --- CTA admin gRPC API server

# SYNOPSIS

**cta-admin-api** [OPTIONS]

**cta-admin-api** \--help

**cta-admin-api** \--version

# DESCRIPTION

**cta-admin-api** serves the administration commands of the CTA frontend (reachable through the `cta-admin` client) over gRPC.

# OPTIONS

**-c**, **\--config** *PATH*

:   Read the main TOML configuration from *PATH*.
    Defaults to `/etc/cta/cta-admin-api.toml`.

**\--config-strict**

:   Reject unknown keys and require every applicable setting explicitly, including settings with defaults.
    Genuinely optional values and sections for disabled features may be absent.

**\--config-check**

:   Validate the configuration and exit without starting the service.
    Respects **\--config-strict**; cannot be combined with **\--runtime-dir**.

**\--runtime-dir** *PATH*

:   Store consumed configuration and version metadata in *PATH*.
    The directory must already exist and be readable and writable by the service.
    Cannot be combined with **\--config-check**.

**-l**, **\--log-file** *PATH*

:   Write logs to *PATH* instead of stdout/stderr.

**-v**, **\--version**

:   Print version information and exit.

**-h**, **\--help**

:   Display command usage information and exit.

# SIGNALS

SIGTERM and SIGINT request service shutdown.
SIGHUP reopens the log file; it does not reload configuration.

# CONFIGURATION

Configuration is read at startup from `/etc/cta/cta-admin-api.toml`, or the file selected by **\--config**.
Restart the service to apply changes.
Use **\--config-check**, with **\--config-strict** when required, to validate a file before starting the service.

The annotated example `/usr/share/doc/cta-admin-api/examples/cta-admin-api.example.toml` is the complete settings reference, including defaults, required values and feature selection.

See the CERN Tape Archive documentation for configuration examples, operational guidance and shared runtime rules.

Select JWT, Kerberos, or both using `auth.enabled_methods`, and supply the corresponding authentication sections.

# FILES

`/etc/cta/cta-admin-api.toml`

:   Default service configuration file.

`/usr/share/doc/cta-admin-api/examples/cta-admin-api.example.toml`

:   Annotated configuration example and complete settings reference.

`/etc/cta/cta-catalogue.conf`

:   Default catalogue connection file, selected by `catalogue.config_file`.

`/etc/cta/cta-scheduler.conf`

:   Default scheduler connection file, selected by `scheduler.config_file`.

`/etc/cta/cta-otel.yaml`

:   Telemetry configuration path used by the example when telemetry is enabled.
    Selected by `telemetry.config_file`.

# SEE ALSO

**cta-workflow-api**(1)

CERN Tape Archive documentation
[https://cta.docs.cern.ch/](https://cta.docs.cern.ch/)
