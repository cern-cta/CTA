---
date: 2026-02-18
section: 1
title: CTA-WORKFLOW-API
header: The CERN Tape Archive (CTA)
---

# NAME

cta-workflow-api --- CTA workflow gRPC API server

# SYNOPSIS

**cta-workflow-api** [OPTIONS]

**cta-workflow-api** \--help

**cta-workflow-api** \--version

# DESCRIPTION

**cta-workflow-api** serves the workflow (EOS) requests of CTA over gRPC: archive, retrieve, cancel retrieve and delete.

# OPTIONS

**-c**, **\--config** *PATH*

:   Read the main TOML configuration from *PATH*.
    Defaults to `/etc/cta/cta-workflow-api.toml`.

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

Configuration is read at startup from `/etc/cta/cta-workflow-api.toml`, or the file selected by **\--config**.
Restart the service to apply changes.
Use **\--config-check**, with **\--config-strict** when required, to validate a file before starting the service.

The annotated example `/usr/share/doc/cta-workflow-api/examples/cta-workflow-api.example.toml` is the complete settings reference, including defaults, required values and feature selection.

See the CERN Tape Archive documentation for configuration examples, operational guidance and shared runtime rules.

Select exactly one authentication method, JWT or mutual TLS, using `auth.enabled_methods`, and supply its configuration section.
With mutual TLS, configure the accepted EOS certificate identities in `[auth.mtls]`.

# FILES

`/etc/cta/cta-workflow-api.toml`

:   Default service configuration file.

`/usr/share/doc/cta-workflow-api/examples/cta-workflow-api.example.toml`

:   Annotated configuration example and complete settings reference.

`/etc/cta/cta-catalogue.conf`

:   Default catalogue connection file, selected by `catalogue.config_file`.

`/etc/cta/cta-scheduler.conf`

:   Default scheduler connection file, selected by `scheduler.config_file`.

`/etc/cta/cta-otel.yaml`

:   Telemetry configuration path used by the example when telemetry is enabled.
    Selected by `telemetry.config_file`.

# SEE ALSO

**cta-admin-api**(1)

CERN Tape Archive documentation
[https://cta.docs.cern.ch/](https://cta.docs.cern.ch/)
