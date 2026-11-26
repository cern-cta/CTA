---
date: 2026-08-20
section: 1
title: CTA-RMCD
header: The CERN Tape Archive (CTA)
---

# NAME

cta-rmcd --- CTA remote media changer daemon

# SYNOPSIS

**cta-rmcd** [OPTIONS]

**cta-rmcd** \--help

**cta-rmcd** \--version

# DESCRIPTION

**cta-rmcd** is the daemon responsible for controlling a SCSI-compatible robotic tape library on behalf of RMC clients such as **cta-smc** and **cta-taped**.

The daemon accepts requests over the RMC protocol and translates them into SCSI media changer operations. Clients can query the library geometry and element status, locate cartridges, mount or dismount cartridges, and import or export cartridges.

By default, **cta-rmcd** listens only on the loopback interface. This supports clients running on the same host without exposing the unauthenticated RMC protocol to the network. Remote clients require the listener to be explicitly configured and protected with suitable network access controls.

# OPTIONS

**-c**, **\--config** *PATH*

:   Read the main TOML configuration from *PATH*.
    Defaults to `/etc/cta/cta-rmcd.toml`.

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

Configuration is read at startup from `/etc/cta/cta-rmcd.toml`, or the file selected by **\--config**.
Restart the service to apply changes.
Use **\--config-check**, with **\--config-strict** when required, to validate a file before starting the service.

The annotated example `/usr/share/doc/cta-rmcd/examples/cta-rmcd.example.toml` is the complete settings reference, including defaults, required values and feature selection.
See the CERN Tape Archive documentation for configuration examples, operational guidance and shared runtime rules.

# FILES

`/etc/cta/cta-rmcd.toml`

:   Default service configuration file.

`/usr/share/doc/cta-rmcd/examples/cta-rmcd.example.toml`

:   Annotated configuration example and complete settings reference.

`/etc/cta/cta-otel.yaml`

:   Telemetry configuration path used by the example when telemetry is enabled.
    Selected by `telemetry.config_file`.

# SEE ALSO

**cta-smc**(1), **cta-taped**(1)

CERN Tape Archive documentation
[https://cta.docs.cern.ch/](https://cta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2026 CERN.
License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an Intergovernmental Organization or submit itself to any jurisdiction.
