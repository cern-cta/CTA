---
date: 2026-08-20
section: 1cta
title: CTA-RMCD
header: The CERN Tape Archive (CTA)
---

# NAME

cta-rmcd --- CTA remote media changer daemon

# SYNOPSIS

**cta-rmcd** [OPTIONS]

**cta-rmcd** --help
**cta-rmcd** --version

# DESCRIPTION

**cta-rmcd** is the daemon responsible for controlling a SCSI-compatible robotic tape library on behalf of RMC clients such as **cta-smc** and **cta-taped**.

The daemon accepts requests over the RMC protocol and translates them into SCSI media changer operations. Clients can query the library geometry and element status, locate cartridges, mount or dismount cartridges, and import or export cartridges.

By default, **cta-rmcd** listens only on the loopback interface. This supports clients running on the same host without exposing the unauthenticated RMC protocol to the network. Remote clients require the listener to be explicitly configured and protected with suitable network access controls.

# OPTIONS

-l, --log-file *PATH*

:   Write logs to *PATH*. If not specified, logs are written to stdout/stderr.

-c, --config *PATH*

:   Path to the main configuration file.
Defaults to */etc/cta/cta-rmcd.toml* if not provided.

--config-strict

:   Treat unknown keys, missing keys, and type mismatches in the configuration file as errors.

--config-check

:   Validate the configuration, then exit.
Respects **--config-strict**.

--runtime-dir *PATH*

:   Store runtime state metadata (such as the consumed configuration and version information) in the specified directory.

-v, --version

:   Print version information and exit.

-h, --help

:   Display command usage information and exit.

# CONFIGURATION

The **cta-rmcd** daemon reads its configuration from a TOML file
(default: */etc/cta/cta-rmcd.toml*).

Each section is described below.

## [media_changer]

device *(default: /dev/smc)*

:   Path to the SCSI-compatible media changer device used to control the tape library.
A device name without a leading slash is resolved below */dev*.

## [rmc_server]

port *(default: 5014)*

:   TCP port on which the daemon listens for RMC client connections.
Clients must be configured to use the same port.

listen_scope *(default: loopback)*

:   Network scope from which the daemon accepts RMC client connections.
Possible values:

- loopback: listen on 127.0.0.1
- any: listen on 0.0.0.0

The RMC protocol provides no authentication. Setting this option to **any** requires appropriate network access controls.
An unsupported value is logged and treated as **loopback**.

## [logging]

level *(default: INFO)*

:   Log mask. Messages below this level are suppressed.
Possible values: EMERG, ALERT, CRIT, ERR, WARNING, NOTICE, INFO, DEBUG.

format *(default: json)*

:   Log output format. Possible values: json, kv.

[logging.attributes]

:   Optional key-value pairs added to all log lines, typically used for monitoring and instance identification.

## [health_server]

enabled *(default: false)*

:   Enable or disable the health server.

host *(default: 127.0.0.1)*

:   Interface to bind to (ignored if using a Unix domain socket).

port *(default: 8080)*

:   TCP port to bind to (ignored if using a Unix domain socket).

use_unix_domain_socket *(default: false)*

:   Expose the health server over a Unix domain socket instead of TCP.
When enabled, **--runtime-dir** must be provided.
The socket file will be created at *<runtime-dir>/health.sock*.

The health server exposes:

* /health/ready
* /health/live

# FILES

*/etc/cta/cta-rmcd.toml*

:   Default configuration file.

*/etc/cta/cta-rmcd.example.toml*

:   Example configuration documenting all available settings.

*/var/log/cta/cta-rmcd.log*

:   Log file used by the packaged systemd service.

*/usr/lib/systemd/system/cta-rmcd.service*

:   Packaged systemd service unit.

# SEE ALSO

**cta-smc**(1cta), **cta-taped**(1cta)

CERN Tape Archive documentation
[https://cta.docs.cern.ch/](https://cta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2026 CERN.
License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an Intergovernmental Organization or submit itself to any jurisdiction.
