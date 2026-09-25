# Tape Drives

Tape drives are the devices responsible for writing data to tape media, and for reading them back.
Just like with the tape media, the drives belong to either the LTO or IBM3592 family, and each drive has a range of supported tape cartridge models.

These tape drives are normally placed inside of a tape library, and connected to a *tape daemon*, which controls it.
At CERN, the tape drives are connected using SCSI, so in these pages we will work with that assumption.

Read and write compatibility can differ between drive and media generations; consult the vendor specifications for the supported combinations. Drives come in full- and half-height models with different capabilities; the examples here assume full-height library drives.

## Drive use and positioning

A drive handles one mounted cartridge at a time. CTA assigns archive or retrieve sessions to available drives, with requests grouped to make useful use of each mount. See [Scheduling](../data-management/scheduling.md) for how work is selected.

Mounting a cartridge and positioning the tape take time. Sustained transfers keep the drive streaming, while gaps in the data supply or frequent positioning reduce efficiency. During retrieval, RAO can reduce positioning time by choosing a better order for the files in a batch.

## Tape Drives and CTA

Each drive has a name in the catalogue and is associated with a [Tape Server](servers.md) and a [logical library](libraries.md#logical-libraries). A separate [Tape Daemon](../components/tape-daemon.md) instance controls each drive and reports its state.

The CTA drive name identifies the resource for scheduling and administration. The library drive address identifies where the robot must load a cartridge, while the device path identifies the drive to the tape server. These identifiers must refer to the same physical drive.

The operator's desired drive state controls whether it should be available for work. The daemon's reported state describes its current activity, such as mounting, transferring, or unloading. These are distinct: a request to take a drive down does not mean its current session has already ended. See [Drive States](../components/tape-daemon.md#drive-states).

## Drive features

Feature availability depends on the drive model, firmware, and CTA configuration.

### Hardware compression

Drives can compress data before recording it and decompress it when reading, without changing the file contents seen by CTA. The capacity gain depends on the data: already-compressed files may gain little, so advertised compressed capacity is not a guaranteed usable capacity.

CTA records the write session's compression setting in the file labels and collects the drive's compression statistics. File sizes and checksums in the catalogue describe the original file data, not its compressed representation on tape.

Drive counters distinguish bytes received from the host from bytes written to tape, providing information about compression. They do not directly give a cartridge occupancy percentage. Physical space usage also includes labels, tape marks, recording overhead, and data that has been logically deleted but remains on tape. Subtracting catalogue file-size totals from nominal tape capacity therefore does not reliably indicate the remaining writable space.

### Encryption

Supported drives can encrypt data as it is written to tape and decrypt it during reads. CTA records the tape's encryption key name in the catalogue; an external key-management integration supplies the key material to the tape daemon, which configures the drive. Reading encrypted media requires access to the corresponding key. See [encryption setup](../../ops/deployment/tape-servers.md#set-up-encryption) for operational guidance.

### Logical block protection (LBP)

LBP checks the integrity of individual blocks exchanged between CTA and the drive. CTA supports CRC32C protection, adding and verifying protection bytes separately from the file payload. This complements the file checksum stored in the catalogue; it does not encrypt data. See [Checksums](media/format.md#checksums) for the recorded format and protection-byte handling.

### Recommended Access Order (RAO)

A capable drive can recommend an order for reading a batch of files that reduces tape positioning time. CTA also provides software ordering, so RAO is not exclusively a hardware feature. See [Recommended Access Order](rao.md) for the concepts and [RAO configuration](../../ops/configuration/tape-daemon.md#recommended-access-order) for setup.

### TapeAlert and drive statistics

TapeAlert flags report conditions such as media problems and cleaning requirements. Drive and volume counters provide additional information for investigating errors and performance; the available statistics depend on the hardware.

The tape daemon reads and logs TapeAlert flags and supported drive and volume statistics. It also aborts a write session if its initial alert check finds conditions classified as critical for writing. See [Logging](../../ops/monitoring/logging.md) for collecting and interpreting CTA logs, and [Tapes, Drives, and Libraries](../../ops/administration/tapes-and-drives.md) for operator intervention.

See [Tape Server Setup](../../ops/deployment/tape-servers.md) for device mappings and configuration, and [Tapes, Drives, and Libraries](../../ops/administration/tapes-and-drives.md) for registration and administration.
