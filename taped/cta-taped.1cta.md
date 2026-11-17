---
date: 2026-08-21
section: 1
title: CTA-TAPED
header: The CERN Tape Archive (CTA)
---

# NAME

cta-taped --- CTA tape drive daemon

# SYNOPSIS

**cta-taped** [OPTIONS]

**cta-taped** \--help

**cta-taped** \--version

# DESCRIPTION

**cta-taped** controls one tape drive and executes archive and retrieve mounts scheduled by CTA.
It manages tape loading and unloading through **cta-rmcd**, transfers data between disk and tape, reports drive and session state, and performs recovery when a session fails.

Run one **cta-taped** process for each tape drive.
The drive and its CTA logical library are selected in the process configuration.

CTA supports SCSI-compatible tape libraries.
The **cta-rmcd** daemon must be reachable by **cta-taped** for physical mount and unmount operations.

# DRIVE LIFECYCLE

The configured startup policy determines whether registration requests desired Up or waits for an operator.
Before scheduling work, the daemon reports Starting and prepares the drive according to its lifecycle configuration.
While reported Down and waiting for desired Up, it leaves the hardware untouched.
An operator Down request lets active work and cleanup finish before the drive is released.

SIGTERM and SIGINT request shutdown; an active tape session finishes before exit.
Crashes, blocked operations and hardware failures can prevent cleanup or catalogue state publication.
See the CERN Tape Archive documentation for startup, shutdown and cleanup guarantees.

# OPTIONS

**-c**, **\--config** *PATH*

:   Read the main TOML configuration from *PATH*.
    Defaults to `/etc/cta/cta-taped.toml`.

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

Configuration is read at startup from `/etc/cta/cta-taped.toml`, or the file selected by **\--config**.
Restart the service to apply changes.
Use **\--config-check**, with **\--config-strict** when required, to validate a file before starting the service.

The annotated example `/usr/share/doc/cta-taped/examples/cta-taped.example.toml` is the complete settings reference, including defaults, required values and feature selection.

See the CERN Tape Archive documentation for configuration examples, operational guidance and shared runtime rules.

# ENVIRONMENT

XrdSecPROTOCOL

:   The XRootD security protocol used for connections to the disk system.
Overridden by **xrootd.security_protocol**.

XrdSecSSSKT

:   Path to the XRootD Simple Shared Secrets keytab.
Overridden by **xrootd.sss_keytab_path**.

# FILES

`/etc/cta/cta-taped.toml`

:   Default service configuration file.

`/usr/share/doc/cta-taped/examples/cta-taped.example.toml`

:   Annotated configuration example and complete settings reference.

`/etc/cta/cta-catalogue.conf`

:   Default catalogue connection file, selected by `catalogue.config_file`.

`/etc/cta/cta-scheduler.conf`

:   Default scheduler connection file, selected by `scheduler.config_file`.

`/etc/cta/cta-otel.yaml`

:   Telemetry configuration path used by the example when telemetry is enabled.
    Selected by `telemetry.config_file`.

# SEE ALSO

**cta-rmcd**(1), **cta-smc**(1)

CERN Tape Archive documentation
[https://cta.docs.cern.ch/](https://cta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2026 CERN.
License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an Intergovernmental Organization or submit itself to any jurisdiction.
