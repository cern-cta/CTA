---
date: 2026-08-21
section: 1cta
title: CTA-TAPED
header: The CERN Tape Archive (CTA)
---

# NAME

cta-taped --- CTA tape drive daemon

# SYNOPSIS

**cta-taped** [OPTIONS]

**cta-taped** --help
**cta-taped** --version

# DESCRIPTION

**cta-taped** controls one tape drive and executes archive and retrieve mounts scheduled by CTA.
It manages tape loading and unloading through **cta-rmcd**, transfers data between disk and tape, reports drive and session state, and performs recovery when a session fails.

Run one **cta-taped** process for each tape drive.
The drive and its CTA logical library are selected in the process configuration.

CTA supports SCSI-compatible tape libraries.
The **cta-rmcd** daemon must be reachable by **cta-taped** for physical mount and unmount operations.

# DRIVE LIFECYCLE

A drive whose reported and desired states are both Down is left untouched, including device probes and robot operations.
At startup, desired Down keeps the drive Down; desired Up is preserved and authorizes preparation cleaning regardless of the previous reported state.
This allows recovery after a crash without discarding a pending operator up request.
A drive without a catalogue entry starts Down.

Preparation publishes CleaningUp before accessing hardware and checks desired state again after cleaning.
An operator down request during preparation or an active session is honoured after hardware cleanup finishes.
Reported Down marks the end of hardware activity; a subsequent up request authorizes another preparation attempt.
Ordinary session completion and handled failures eject the cartridge when the drive and robot permit it.
A successful eject does not necessarily make the drive reusable: configuration-reset failures can still require Down.

Controlled exits attempt both reported Down and desired Down once drive identity has been validated, including failures before startup becomes ready.
They preserve existing operator or failure reasons and return failure if down publication fails.
Shutdown does not perform additional hardware cleanup or wait indefinitely for database recovery.
Configuration failures before catalogue access and failures to validate drive ownership cannot guarantee down publication.

SIGTERM requests an exit between scheduling iterations or polling attempts.
An active tape session finishes before exit; sleeps and blocking operations are not interrupted.
Full graceful shutdown and recovery from partial worker startup or worker join failures are not implemented by this lifecycle guarantee.
Crashes, allocation or logging failures, stuck cartridges, and robot communication failures can prevent cleanup or state publication.

# OPTIONS

-l, --log-file *PATH*

:   Write logs to *PATH*.
If not specified, logs are written to stdout/stderr.

-c, --config *PATH*

:   Path to the main configuration file.
Defaults to */etc/cta/cta-taped.toml* if not provided.

--config-strict

:   Treat unknown keys, missing keys, and type mismatches in the configuration file as errors.

--config-check

:   Validate the configuration, then exit.
Respects **--config-strict**.

--runtime-dir *PATH*

:   Store runtime state metadata, such as the consumed configuration and version information, in the specified directory.

-v, --version

:   Print version information and exit.

-h, --help

:   Display command usage information and exit.

# CONFIGURATION

The **cta-taped** daemon reads its configuration from a TOML file
(default: */etc/cta/cta-taped.toml*).

Each section is described below.

## [drive]

name

:   Unique CTA name of the tape drive.
The name is included in log records and exposed to CTA tooling.

device

:   Path to the non-rewinding character device used to access the drive.
Use a persistent device symlink, such as one under */dev/tape/by-id*.
Site-specific udev rules may provide other persistent paths, such as */dev/tape/by-name/<drive-name>*.

control_path

:   SCSI media-changer address associated with the drive.
It is normally `smc` followed by the drive ordinal reported by **cta-smc -q D**, for example `smc0`.

logical_library_name

:   Name of the existing CTA logical library to which the drive belongs.

## [catalogue]

config_file

:   Path to the CTA catalogue configuration file
(commonly */etc/cta/cta-catalogue.conf*).

## [scheduler]

backend_name

:   Unique identifier for the backend scheduler resources.
Example structure: `[ceph|postgres|vfs][User|Repack]`.

config_file

:   Path to the CTA scheduler configuration file
(commonly */etc/cta/cta-scheduler.conf*).

tape_cache_max_age_secs *(default: 600)*

:   Maximum age of cached tape statistics used when deciding whether work warrants a mount.

retrieve_queue_cache_max_age_secs *(default: 10)*

:   Maximum age of cached retrieve-queue statistics.

## [mounts]

Session timeout checks affect liveness only; they do not terminate or recover sessions.
Preparing, Finalizing and controller operations remain unchecked.

minimum_queued_bytes *(default: 500000000000)*

:   Minimum queued bytes required before the scheduler considers an archive or retrieve queue large enough to mount.
This is combined with **minimum_queued_files** using OR semantics.
A mount can still start when the applicable catalogue mount-rule timeout expires.

minimum_queued_files *(default: 10000)*

:   Minimum queued files required before the scheduler considers an archive or retrieve queue large enough to mount.
This is combined with **minimum_queued_bytes** using OR semantics.

get_next_mount_timeout_secs *(default: 900)*

:   Maximum time allowed for selecting the next mount.
On expiry, a warning is logged and the request is retried.

idle_scheduling_interval_secs *(default: 10)*

:   Delay before retrying after the scheduler reports that no mount is available.

drive_state_poll_interval_secs *(default: 5)*

:   Delay before polling the catalogue for the desired drive state again while the drive is Down.
Lower values make **cta-taped** react faster when an operator sets the drive Up but increase catalogue polling.

logical_library_poll_interval_secs *(default: 5)*

:   Delay before checking again for a logical library that does not yet exist.

mount_timeout_secs *(default: 600)*

:   Maximum time in Mounting before liveness becomes unhealthy.

tape_load_timeout_secs *(default: 300)*

:   Maximum time in Loading before liveness becomes unhealthy.
Also limits the existing physical drive readiness wait, whose failure starts cleanup.

tape_unload_timeout_secs *(default: 900)*

:   Maximum time in Unloading before liveness becomes unhealthy.
Must be positive and nonzero.

unmount_timeout_secs *(default: 900)*

:   Maximum time in Unmounting before liveness becomes unhealthy.

## [transfers]

buffer_count *(default: 5000)*

:   Number of in-memory buffers allocated to the data-transfer pipeline.

buffer_size_bytes *(default: 5000000)*

:   Size of each in-memory transfer buffer.
Approximate transfer-cache memory consumption is **buffer_count** multiplied by **buffer_size_bytes**.

disk_io_threads *(default: 10)*

:   Number of disk I/O workers and therefore the maximum number of transfers that can perform disk I/O concurrently.

stats_report_interval_secs *(default: 15)*

:   Interval in seconds between periodic tape-session statistics reports to the scheduler and logs.
Must be greater than zero.
Final statistics are reported when the session finishes, without waiting for this interval.

no_block_move_timeout_secs *(default: 1800)*

:   Maximum inactivity in Transferring before liveness becomes unhealthy.
Measured from the later of state entry and last block movement; reporter activity does not count.
Also preserves the existing stuck-file warning interval.

## [transfers.archive]

fetch_max_bytes *(default: 100000000000)*

:   Maximum total bytes requested from the scheduler in one archive batch.
The batch is limited when either this value or **fetch_max_files** is reached.

fetch_max_files *(default: 5000)*

:   Maximum number of files requested from the scheduler in one archive batch.

flush_max_bytes *(default: 32000000000)*

:   Maximum bytes written between flushes to tape using a synchronized tape mark.
A flush occurs on a file boundary, so the actual byte count can exceed this value.

flush_max_files *(default: 200)*

:   Maximum files written between flushes to tape.
A flush is triggered when either this value or **flush_max_bytes** is reached.

## [transfers.archive.underfill]

watch_period_secs *(default: 300)*

:   Minimum duration for which archive batches must remain underfilled before an unmount can be requested.

minimum_samples *(default: 3)*

:   Minimum number of underfilled archive batches required during the observation period before an unmount can be requested.

start_threshold_percent *(default: 40)*

:   Fill percentage below which an underfill observation begins.
Valid values are 0 through 100.

recovery_threshold_percent *(default: 60)*

:   Fill percentage at or above which an underfill observation is cancelled.
Valid values are 0 through 100 and this value must be greater than **start_threshold_percent**.

The effective fill percentage is the greater of the fetched-file and fetched-byte percentages.
An archive session ends for underfill only after both **watch_period_secs** and **minimum_samples** are satisfied.

## [transfers.retrieve]

fetch_max_bytes *(default: 100000000000)*

:   Maximum total bytes requested from the scheduler in one retrieve batch.
The batch is limited when either this value or **fetch_max_files** is reached.

fetch_max_files *(default: 5000)*

:   Maximum number of files requested from the scheduler in one retrieve batch.

drain_to_disk_timeout_secs *(default: 1800)*

:   Maximum time in DrainingToDisk before liveness becomes unhealthy.

external_free_disk_space_script *(default: /usr/bin/cta-eosdf.sh)*

:   Path to an operator-provided executable that reports free space in the retrieve destination.

## [transfers.retrieve.rao]

enabled *(default: true)*

:   Enable Recommended Access Order on supported drives.
Hardware RAO is preferred when available; otherwise software RAO may be used.

lto_algorithm *(default: sltf)*

:   Software RAO algorithm for LTO-8 drives.
Possible values are `linear`, `random`, and `sltf` (Shortest Locate Time First).
The `sltf` algorithm requires the corresponding media type's `NB_WRAPS`, `MIN_LPOS`, and `MAX_LPOS` catalogue columns to be populated.

## [transfers.encryption]

enabled *(default: true)*

:   Enable tape-drive hardware encryption when a key is configured for the tape or tape pool.

external_key_script *(default: /usr/local/bin/cta-get-encryption-key.sh)*

:   Path to the operator-provided executable used to obtain encryption keys.

## [rmcd]

host *(default: localhost)*

:   Hostname or IP address of the Remote Media Changer Daemon.
RMCD traffic is neither encrypted nor authenticated, so remote connections are not recommended.

port *(default: 5014)*

:   TCP port on which RMCD accepts requests.

request_timeout_secs *(default: 600)*

:   Timeout applied to network read and write operations with RMCD.

request_attempts *(default: 10)*

:   Maximum number of attempts for a retriable RMC request.
Network and protocol errors, including expiration of **request_timeout_secs**, are not retried by this setting.

## [logging]

level *(default: INFO)*

:   Log mask.
Messages below this level are suppressed.
Possible values: EMERG, ALERT, CRIT, ERR, WARNING, NOTICE, INFO, DEBUG.

format *(default: json)*

:   Log output format.
Possible values: json, kv.

[logging.attributes]

:   Optional key-value pairs added to all log lines, typically used for monitoring and instance identification.

## [telemetry]

config_file

:   Path to the OpenTelemetry SDK declarative configuration file.
If omitted or empty, telemetry is disabled.

on_init_failure *(default: warn)*

:   Behaviour if telemetry initialisation fails.
Possible values: `warn`, `fatal`.

Telemetry is experimental and disabled by default unless explicitly enabled under **[experimental]**.

## [health_server]

enabled *(default: false)*

:   Enable or disable the health server.

host *(default: 127.0.0.1)*

:   Interface to bind to when using TCP.

port *(default: 8080)*

:   TCP port to bind to when using TCP.

use_unix_domain_socket *(default: false)*

:   Expose the health server over a Unix domain socket instead of TCP.
When enabled, **--runtime-dir** must be provided.
The socket file is created at *<runtime-dir>/health.sock*.

The health server exposes:

* /health/ready
* /health/live

## [xrootd]

security_protocol *(default: sss)*

:   Override `XrdSecPROTOCOL` for connections to the disk system.

sss_keytab_path

:   Override `XrdSecSSSKT` with the path to the XRootD Simple Shared Secrets keytab.
The file must be readable by **cta-taped** when the `sss` protocol is selected.

## [experimental]

telemetry_enabled *(default: false)*

:   Enable experimental telemetry support configured under **[telemetry]**.

# ENVIRONMENT

XrdSecPROTOCOL

:   The XRootD security protocol used for connections to the disk system.
Overridden by **xrootd.security_protocol**.

XrdSecSSSKT

:   Path to the XRootD Simple Shared Secrets keytab.
Overridden by **xrootd.sss_keytab_path**.

# FILES

*/etc/cta/cta-taped.toml*

:   Default configuration file.

*/etc/cta/cta-catalogue.conf*

:   CTA catalogue configuration file.

*/etc/cta/cta-scheduler.conf*

:   CTA scheduler configuration file.

*/etc/cta/cta-otel.yaml*

:   OpenTelemetry declarative configuration file used when telemetry is enabled.

# SEE ALSO

**cta-rmcd**(1cta), **cta-smc**(1cta)

CERN Tape Archive documentation
[https://cta.docs.cern.ch/](https://cta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2026 CERN.
License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an Intergovernmental Organization or submit itself to any jurisdiction.
