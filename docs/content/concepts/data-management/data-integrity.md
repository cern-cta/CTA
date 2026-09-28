# Data Integrity

CTA checks file contents during archival, retrieval, and repack using expected sizes and checksums. These checks complement the tape drive's own error detection and optional logical block protection. They establish whether transferred data matches the expected contents; they do not replace additional copies, catalogue backups, or verification of stored tapes.

## File identity and expected contents

The disk system supplies the file identity, size, and checksum with an archive request. CTA assigns an archive file ID and records successful tape copies in the catalogue. The identity associates the work with a file; the size and checksum describe its expected contents.

File sizes and checksums describe the original file data, independent of tape position or drive compression. Moving a copy through repack changes its location, not the expected contents. See [Storage Model](storage-model.md) for archive files and tape copies.

The disk system must supply correct metadata and keep the source file unchanged while archival reads it. A checksum comparison detects disagreement with the supplied value; it cannot establish that the original file was correct before that value was calculated.

## Checks during transfers

The tape-transfer path computes an **Adler-32** checksum over each file. The expected checksum accompanies the archive request and is retained in the catalogue for subsequent reads.

| Workflow | Checks and responsibilities |
| --- | --- |
| **Archival** | The tape daemon checks the source file's size against the request and computes a checksum while processing its data for tape. The calculated checksum must match the expected value before the copy is accepted into the catalogue. |
| **Retrieval** | The tape daemon calculates the checksum while reading the file from tape and compares it with the catalogue value. A mismatch fails the transfer rather than producing a successful retrieval result. |
| **Repack** | The same read and write checks apply as data passes through the repack buffer. Destination copies retain the file's identity and expected contents. Operator-supplied recovered files also undergo the archival checks. |

Successful archival does not mean CTA has performed a second, full read-back of the file from tape. Its transfer checks and the drive's write-error handling are distinct from a later verification read.

The disk system controls when a received replica becomes available to clients and how transfer failures appear in its namespace or request state. See [Archival](archival.md) and [Retrieval](retrieval.md) for completion handling.

## File checksums and block protection

The whole-file checksum and logical block protection (LBP) cover different units:

- **File checksum:** Adler-32 describes the complete file contents and is stored in the catalogue. It allows CTA to compare a later read with the expected file data.
- **Logical block protection:** tapes labelled with CRC32C LBP carry additional protection bytes for each block. CTA automatically enables the corresponding drive protection when reading or writing those tapes. These bytes are checked separately and removed before the block contents are passed back to the file reader.

LBP protection bytes are not part of the original file payload and are not included in its logical size. LBP complements the whole-file checksum and the drive's internal media protection; it does not replace either. See [CTA Tape Format: Checksums](../tape/media/format.md#checksums) for the on-tape representation.

## Verification and mismatches

A size or checksum mismatch prevents the affected transfer from being considered successful. Depending on the failure and retry handling, work can be retried or eventually reported as failed. The mismatch alone does not identify the cause: source data, expected metadata, the transfer path, or tape hardware may need investigation.

**Tape verification** deliberately reads selected tape files and checks their contents without creating client-facing disk replicas. It can reveal problems before a client needs the data, but verifies only the files actually read; it does not imply continuous checking of every stored copy. See the [Tape Verification Framework](../../ops/tools/tape-verification.md) for operator tooling.

Detection is separate from recovery. A failed check does not automatically repair a copy or establish that every copy of the file is damaged. Recovery requires assessing the available copies and, where necessary, writing a replacement from a verified source.

## Zero-length files

Empty files receive special treatment at archive submission (`CLOSEW`). The Workflow Frontend can be configured to reject them, with exemptions for specified VOs. When they are permitted, it returns success without queueing a tape write. The scheduler itself rejects zero-length archive jobs.

An accepted empty-file submission therefore does not create a tape copy or produce the usual tape-transfer completion workflow. Allocation of an archive file ID during registration is not evidence that a copy exists. The disk-system integration must handle the empty file's namespace representation and availability without relying on a later tape retrieval.
