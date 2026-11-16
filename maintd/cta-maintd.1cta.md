---
date: 2026-02-18
section: 1
title: CTA-MAINTD
header: The CERN Tape Archive (CTA)
---

# NAME

cta-maintd --- CTA maintenance daemon

# SYNOPSIS

**cta-maintd** [OPTIONS]

**cta-maintd** \--help

**cta-maintd** \--version

# DESCRIPTION

**cta-maintd** is the daemon responsible for periodically executing a set of routines related to the CTA scheduler, catalogue and disk system.

These routines include:

* Reporting archive and retrieve job status to the disk system.
* Expanding and reporting repack requests.
* Garbage collection and queue cleanup (depending on the scheduler backend).

All enabled routines are executed sequentially in a cycle:

A -> B -> C -> sleep -> A -> B -> C -> sleep ...

A cycle consists of executing each enabled routine once.
The sleep interval between cycles is configurable.
It is not possible to define different sleep intervals per routine within a single process.
If per-routine intervals are required, run multiple **cta-maintd** instances with different subsets of routines enabled.

# OPTIONS

**-c**, **\--config** *PATH*

:   Read the main TOML configuration from *PATH*.
    Defaults to `/etc/cta/cta-maintd.toml`.

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

Configuration is read at startup from `/etc/cta/cta-maintd.toml`, or the file selected by **\--config**.
Restart the service to apply changes.
Use **\--config-check**, with **\--config-strict** when required, to validate a file before starting the service.

The annotated example `/usr/share/doc/cta-maintd/examples/cta-maintd.example.toml` is the complete settings reference, including defaults, required values and feature selection.
The packaged example matches the compiled scheduler backend; the online guide provides both objectstore and PostgreSQL variants.
See the CERN Tape Archive documentation for configuration examples, operational guidance and shared runtime rules.

# ENVIRONMENT

XrdSecPROTOCOL

:   The XRootD security protocol used for connections to the disk system.
Overridden by **xrootd.security_protocol**.

XrdSecSSSKT

:   Path to the XRootD Simple Shared Secrets keytab.
Overridden by **xrootd.sss_keytab_path**.

# FILES

`/etc/cta/cta-maintd.toml`

:   Default service configuration file.

`/usr/share/doc/cta-maintd/examples/cta-maintd.example.toml`

:   Annotated configuration example and complete settings reference.

`/etc/cta/cta-catalogue.conf`

:   Default catalogue connection file, selected by `catalogue.config_file`.

`/etc/cta/cta-scheduler.conf`

:   Default scheduler connection file, selected by `scheduler.config_file`.

`/etc/cta/cta-otel.yaml`

:   Telemetry configuration path used by the example when telemetry is enabled.
    Selected by `telemetry.config_file`.

# SEE ALSO

CERN Tape Archive documentation
[https://cta.docs.cern.ch/](https://cta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2026 CERN.
License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an Intergovernmental Organization or submit itself to any jurisdiction.
