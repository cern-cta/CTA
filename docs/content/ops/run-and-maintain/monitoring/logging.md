# Logging

Logging is a crucial part of CTA that allows operators to monitor and troubleshoot the various CTA components. CTA services can be configured to log to `stdout` or to a file.

For host log directories, packaged rotation policies, reopen signals, and the differences in containers, see [RPM Packages and Services](../../deploy-and-configure/deployment/installation/rpm-packages.md#logging-and-rotation).

## Log Levels

CTA has the following log levels (in order of decreasing severity):

- `EMERG`
- `ALERT`
- `CRIT`
- `ERR`
- `WARNING`
- `NOTICE`
- `INFO`
- `DEBUG`

Each CTA service can be configured to filter out log messages based on the minimum desired log level.

## Log Formats

CTA supports two logging formats:

- key-value (default)
- JSON

It is highly recommended when deploying CTA to configure it to use JSON as this allows for easy parsing by monitoring tools.

## Log Schema

!!! info

    The logging schema is a work in progress. Consult the schema shipped with this release for the covered attributes and events. While the log schema version is < 1.0, changes may be frequent.

CTA services come bundled with a `cta-logging.schema.json` file. This file follows the [jsonschema](https://json-schema.org/) specifications to define a schema for the log messages. The goal of this schema is to guide operators in what events to monitor and to give them guarantees on the output of CTA to ensure upgrades don't break existing monitoring.

The schema does not fully describe every log message in detail, it only describes the following:

- **Resource Attributes**: attributes present in every single log messages.
- **Events and their attributes**: certain events in CTA and what attributes are present when this event is logged.

Due to the way log context propagation is handled internally in CTA, it is difficult to specify the exact format of each and every log messages. As such, the properties defined in the log schema describe the **minimal** set of attributes you would find for a log message. Additional properties may exist, but if they are not in the schema, they should not be relied upon.

## JSON Logging

Every CTA service except for `cta-rmcd` supports JSON logs. It is **highly recommended to configure CTA to produce JSON logs** as these should be natively processed by modern monitoring tools.

## Tape session logging

Tape session logs describe both the daemon's transfer outcome and the hardware diagnostics collected from the drive.
The [tape daemon concepts](../../../concepts/components/tape-daemon.md#tape-session-failures-and-counters) explain what constitutes a session failure and how that differs from drive reusability.
The field names below describe the current implementation; consult the [log schema](#log-schema) when building monitoring integrations.

### Final session summary

The daemon emits `Tape session finished` when session finalization reaches completion, including cleanup and final reporting.
Periodic `Tape session statistics` records describe progress before that point.
An abrupt crash or fatal failure can prevent the final record from being emitted; a missing completion record must not be interpreted as success.

| Fields | How to interpret them |
| --- | --- |
| `status` | `in_progress` in periodic reports; `success` or `failure` in the final report. Any recorded session failure makes the final status `failure`, including cleanup or reporting failures. |
| `sessionState`, `sessionType` | The session's current phase and transfer type. |
| `mountId`, `tapeVid`, `mountType` | Identify the scheduled mount, cartridge, and kind of work. Use these with the drive's log context to correlate the summary, worker errors, and SCSI records. |
| `mountAttempted`, `wasTapeMounted` | Whether a physical mount was attempted and whether mount timing was recorded, respectively. These fields do not establish that the cartridge was subsequently cleaned up successfully. |
| `filesCount`, `dataVolume`, `headerVolume` | Transfer counts and byte volumes, alongside separate user, repack, and verification counts. These are not a replacement for individual file outcome reports. |
| `initialMountTime`, `tapeLoadTime`, `readWriteTime`, `flushTime`, `unloadTime`, `unmountTime`, `cleanupTime` | Timing measurements for mounting, loading, tape I/O, flushing, and cleanup. |
| `waitFreeMemoryTime`, `waitDataTime`, `waitInstructionsTime`, `waitReportingTime`, `drainingTime` | Time spent waiting within the transfer pipeline or draining retrieved data to disk. |
| `payloadTransferSpeedMBps`, `driveTransferSpeedMBps` | Calculated transfer rates for payload and payload plus tape headers, respectively. |

The final summary also contains the nonzero counters described below.
Session failure does not necessarily mean that every file failed or that the drive is unusable; inspect individual transfer outcomes and the drive's state and down reason separately.

### Tape session counters

Failure counters, informational events, and tape alerts are recorded separately.
Only nonzero counters are included in session statistics records.

#### Failure counter fields

These counters identify the operation that failed, rather than counting failed files or independently identifying root causes.
One file can encounter several failures, such as a transfer error followed by a reporting error.
Propagating an already classified error between workers does not by itself require another failure count.

| Operation | Log counters | Meaning |
| --- | --- | --- |
| Reading from disk | `Error_diskOpenForRead`, `Error_diskRead` | Opening or reading an archive source file failed. |
| Checking source size | `Error_diskFileToReadSizeMismatch`, `Error_diskUnexpectedSizeWhenReading` | The source size differs from the expected size, either at the initial check or while reading. |
| Writing to disk | `Error_diskOpenForWrite`, `Error_diskWrite`, `Error_diskCloseAfterWrite` | Opening, writing, or closing a retrieval destination failed. |
| Mounting and loading | `Error_tapeMountForRead`, `Error_tapeMountForWrite`, `Error_tapeLoad` | Mounting the cartridge or waiting for the drive to load it failed. |
| Checking tape suitability | `Error_checkingTapeAlert`, `Error_tapeNotWriteable`, `Error_tapesCheckLabelBeforeReading` | A tape-alert check, writeability check, or label check prevented tape access. |
| Configuring the drive | `Error_tapeEncryptionEnable`, `Error_tapeEncryptionDisable`, `Error_tapeLbpDisable` | Enabling or clearing encryption, or disabling logical block protection, failed. |
| Positioning and file sequence | `Error_tapePositionForRead`, `Error_tapePositionForWrite`, `Error_tapeFSeqOutOfSequenceForWrite` | Positioning for tape access failed, or the write file sequence was inconsistent. |
| Reading tape | `Error_tapeReadData` | Reading a file from tape failed. |
| Writing tape | `Error_tapeWriteHeader`, `Error_tapeWriteData`, `Error_tapeWriteTrailer`, `Error_tapeFlush` | Writing a file's header, data, or trailer, or flushing the drive's internal write buffer, failed. |
| Skipping an archive file | `Info_fileSkipped` | A file was not archived. Despite the `Info_` prefix, this is a failure counter and makes the session unsuccessful. |
| Physical cleanup | `Error_tapeUnload`, `Error_tapeDismount`, `Error_unexpectedCleanup` | Unloading, returning the cartridge to the library, or another cleanup operation failed. |
| Reporting | `Error_reporting` | Recording or publishing session activity, transfer outcomes, completion, or statistics failed. This can make a session unsuccessful even after data transfer and cleanup succeeded. |
| Supplying work and coordinating workers | `Error_taskInjection`, `Error_workerSignalling` | Supplying transfer tasks or signalling a worker failed. |
| Otherwise unclassified failures | `Error_unexpectedSession`, `Error_unclassifiedFile` | A session or file operation failed without a more specific recorded classification. |

#### Informational event and tape-alert fields

The following events do not by themselves make a session fail.
A session that transfers no files can therefore finish successfully if no failure was recorded.

| Log counters | Meaning |
| --- | --- |
| `Info_diskSpaceReservationTestFailure`, `Info_diskSpaceReservationFailure` | The disk-space reservation test or reservation did not succeed, preventing the associated retrieval work from proceeding. |
| `Info_noFilesToRecall`, `Info_noFilesToMigrate`, `Info_emptyMount` | No work was available for the session or mount. |
| `Info_tapeFilledUp` | The tape reached capacity. This is recorded once per session, rather than counting every subsequent observation. |

Tape alerts are counted separately by alert code and logged as `Error_<alert name>`.
Recording an alert alone does not mark the session as failed, despite that prefix.
If an alert check rejects an operation, the corresponding operation failure is also recorded and makes the session unsuccessful.
Use the final session status and failure categories rather than the `Error_` or `Info_` prefix alone to interpret the outcome.

### End-of-session SCSI metrics

As the tape worker finishes, the daemon queries the drive for SCSI statistics before unloading and unmounting the cartridge.
These hardware diagnostics complement the [tape session failure counters](#tape-session-counters); they are emitted in separate log records rather than only in the final session summary.
They help distinguish problems associated with the drive, cartridge, or interface, including errors the hardware recovered without failing a transfer.

| Log message | What it records | Example fields |
| --- | --- | --- |
| `Logging mount general statistics` | Corrected and uncorrected errors and bytes processed for the session's transfer direction: read statistics for retrieval, write statistics for archival. Also includes non-medium errors reported by the drive. | `mountTotalCorrectedReadErrors`, `mountTotalUncorrectedReadErrors`, `mountTotalReadBytesProcessed`; corresponding `Write` fields for archival; `mountTotalNonMediumErrorCounts`. |
| `Logging drive statistics` | Drive-reported quality and efficiency indicators, retry counts, and transient errors. Depending on the drive, quality indicators cover the drive, medium, interfaces, library, and read/write operations, for the current mount or over its lifetime. | `mountDriveEfficiencyPrct`, `mountMediumEfficiencyPrct`, `mountReadEfficiencyPrct`, `mountWriteEfficiencyPrct`, `lifetimeMediumEfficiencyPrct`, `mountTotalReadRetries`, `mountTotalWriteRetries`, `mountReadTransients`, `mountWriteTransients`, `mountServoTransients`. |
| `Logging volume statistics` | Cartridge history: lifetime mount counts, recovered and unrecovered read/write errors, manufacturing date, and passes over the beginning or middle of the tape where supported. | `lifetimeVolumeMounts`, `lifetimeVolumeRecoveredReadErrors`, `lifetimeVolumeUnrecoveredReadErrors`, corresponding `Write` fields, `volumeManufacturingDate`, `lifetimeBOTPasses`, `lifetimeMOTPasses`, `validity`. |

The records include `driveManufacturer`, `driveType`, `firmwareVersion`, and `serialNumber`, alongside the tape session's log context.
The available fields and their interpretation depend on the drive model and supported SCSI log pages; the examples above are not guaranteed on every drive.
Fields prefixed with `mount` describe the current mount, while `lifetime` fields describe cumulative history and must not be treated as errors newly introduced by this session.
Efficiency fields ending in `Prct` are percentages reported or derived from the drive's quality indicators, not the daemon's measured transfer throughput.

Collecting a nonzero hardware error or retry count does not itself mark the tape session as failed or set desired DOWN.
The session's recorded operation failures determine its outcome; corrected hardware errors can coexist with a successful session.
These statistics are collected before physical cleanup and therefore do not describe the outcome of the subsequent unload or unmount.

Collection is best effort: an unavailable statistics group produces `SCSI Statistics could not be acquired from drive`, and a failed query can produce an `Exception in logging ... statistics` message.
The daemon still attempts the other groups and physical cleanup when a statistics query fails.
Missing statistics do not mean that the corresponding counters were zero.

