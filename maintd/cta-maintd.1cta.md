---
date: 2026-02-18
section: 1cta
title: CTA-MAINTD
header: The CERN Tape Archive (CTA)
---

# NAME

cta-maintd --- CTA maintenance daemon

# SYNOPSIS

**cta-maintd** [OPTIONS]

**cta-maintd** --help
**cta-maintd** --version

# DESCRIPTION

**cta-maintd** is the daemon responsible for periodically executing a set of routines related to the CTA scheduler, catalogue and disk system.

These routines include:

* Reporting archive and retrieve job status to the disk buffer.
* Expanding and reporting repack requests.
* Garbage collection and queue cleanup (depending on the scheduler backend).

All enabled routines are executed sequentially in a cycle:

A -> B -> C -> sleep -> A -> B -> C -> sleep ...

A cycle consists of executing each enabled routine once. The sleep interval between cycles is configurable. It is not possible to define different sleep intervals per routine within a single process. If per-routine intervals are required, multiple **cta-maintd** instances should be started, each with a different subset of routines enabled.

# OPTIONS

-l, --log-file *PATH*

:   Write logs to *PATH*. If not specified, logs are written to stdout/stderr.

-c, --config *PATH*

:   Path to the main configuration file.
Defaults to */etc/cta/cta-maintd.toml* if not provided.

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

# ROUTINES

The available routines depend on the configured scheduler backend.

## Common routines

disk_report_archive

:   Reports archive job success or failure to the disk system.

disk_report_retrieve

:   Reports retrieve job success or failure to the disk system.

repack_expand

:   Expands repack requests into individual archive and retrieve jobs.

repack_report

:   Handles reporting for repack-generated requests.

## Objectstore-specific routines

queue_cleanup

:   Finds queues marked for cleanup, takes ownership, and moves requests to other queues.

garbage_collect

:   Performs garbage collection on stale agents and objects in the objectstore.

## Postgres-specific routines

Additional queue cleanup routines may be enabled when using the Postgres scheduler backend, including:

* user_active_queue_cleanup
* repack_active_queue_cleanup
* user_pending_queue_cleanup
* repack_pending_queue_cleanup
* scheduler_maintenance_cleanup

# CONFIGURATION

The **cta-maintd** daemon reads its configuration from a TOML file
(default: */etc/cta/cta-maintd.toml*).

Each section is described below.

## [catalogue]

config_file

:   Path to the CTA catalogue configuration file
(commonly */etc/cta/cta-catalogue.conf*).

## [scheduler]

backend_name

:   Unique identifier for the backend scheduler resources.
Example structure: [ceph|postgres|vfs][User|Repack].

objectstore_backend_path

:   URL of the objectstore (CTA Scheduler Database).
Typically a Ceph RADOS URL. A file-based backend may be used for testing.

tape_cache_max_age_secs *(default: 600)*

:   Maximum age of tape cache entries.

retrieve_queue_cache_max_age_secs *(default: 10)*

:   Maximum age of retrieve queue cache entries.

## [logging]

level *(default: INFO)*

:   Log mask. Messages below this level are suppressed.
Possible values: EMERG, ALERT, CRIT, ERR, WARNING, NOTICE, INFO, DEBUG.

format *(default: json)*

:   Log output format. Possible values: json, kv.

[logging.attributes]

:   Optional key-value pairs added to all log lines, typically used for monitoring and instance identification.

## [telemetry]

config_file

:   Path to the OpenTelemetry SDK declarative configuration file.
If omitted or empty, telemetry is disabled.

on_init_failure *(default: warn)*

:   Behaviour if telemetry initialisation fails.
Possible values:
- warn
- fatal

Telemetry is experimental and disabled by default unless explicitly enabled under **[experimental]**.

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

## [xrootd]

security_protocol *(default: sss)*

:   Overrides XrdSecPROTOCOL.

sss_keytab_path

:   Overrides XrdSecSSSKT.

## [routines]

cycle_sleep_interval_secs *(default: 10)*

:   Sleep duration between cycles of routine execution.

max_cycle_duration_secs *(default: 900)*

:   Maximum allowed duration of a full cycle.
If exceeded, the cycle is not interrupted, but the process will no longer be considered alive by the health server.

Each routine can be individually configured and enabled/disabled, for example:

* disk_report_archive = { enabled = true, batch_size = 500, soft_timeout_secs = 30 }
* disk_report_retrieve = { enabled = true, batch_size = 500, soft_timeout_secs = 30 }
* repack_expand = { enabled = true, max_to_expand = 2 }
* repack_report = { enabled = true, soft_timeout_secs = 30 }
* queue_cleanup = { enabled = true, batch_size = 500 }
* garbage_collect = { enabled = true }

## [experimental]

telemetry_enabled *(default: false)*

:   Enables experimental telemetry support.

# FILES

*/etc/cta/cta-maintd.toml*

:   Default configuration file.

*/etc/cta/cta-catalogue.conf*

:   CTA catalogue configuration file.

/etc/cta/cta-otel.yaml

:   OpenTelemetry declarative configuration file. Used only if telemetry is enabled and a config_file is specified under the [telemetry] section.

# SEE ALSO

CERN Tape Archive documentation
[https://cta.docs.cern.ch/](https://cta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2026 CERN.
License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an Intergovernmental Organization or submit itself to any jurisdiction.
