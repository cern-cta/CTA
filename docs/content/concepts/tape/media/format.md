# CTA Tape Format

This page describes the records written in the CTA format, which uses the AUL layout inherited from CASTOR. See [Tape Labels](labels.md) for terminology, label purposes, and standards background.

The CASTOR hex dumps below are historical examples, not byte-for-byte examples of current CTA output.

Labels contain 80 bytes of ASCII text, beginning with a four-character identifier; unused fields are padded with spaces (`0x20`). In the layouts below, `TM` denotes a tape mark.

All byte ranges below are zero-based and inclusive. Label payloads are 80 bytes; when logical block protection is enabled, the drive interface handles the additional protection bytes.

| VOL1 | HDR1 \| HDR2 \| UHL1 \| TM \| DATA \| TM \| EOF1 \| EOF2 \| UTL1 \| TM |
| ---- | ---------------------------------------------------------------------- |
|      | <-------------------------- One data file ---------------------------------------------------> |

Volumes that have just been initialised contain no data records, just a single ‘header label group’:

| VOL1 | HDR1(PRELABEL) \| TM |
| ---- | -------------------- |
|      |                      |

The first archive write replaces the PRELABEL header after VOL1 with the first file's header group.

## Volume Label (VOL*n*)

The very first label record on a labelled volume is VOL1.
If this label is incorrect, you will not advance at all.

### The structure of the volume label (VOL1)

| Bytes | Length | Offset | Content |
| ----- | ------ | ------ | ------- |
|  0-3  |    4   |  0x00  | Volume label indicator: the characters VOL1 |
|  4-9  |    6   |  0x04  | Volume serial number (VSN) (e.g., “AB1234”) |
|   10  |    1   |  0x0A  | Accessibility (left as empty space)         |
| 11-23 |    13  |  0x0B  | Reserved (spaces)                           |
| 24-36 |    13  |  0x18  | Implementation identifier (left as empty spaces) |
| 37-50 |    14  |  0x25  | Owner identifier: “CTA”, padded with spaces |
| 51-76 |    26  |  0x33  | Reserved (spaces) |
| 77-78 |     2  |  0x4D  | Logical block protection method: ASCII `00` (disabled) or `02` (CRC32C) for newly labelled tapes |
|   79  |    1   | 0x4F   | Label standard level: ASCII `3` |

### Examples

#### An example of the beginning of the tape

``` hexdump
00000000 56 4f 4c 31 56 35 32 30 30 31 20 20 20 20 20 20 |VOL1V52001      |
00000010 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 |                |
00000020 20 20 20 20 20 43 41 53 54 4f 52 20 20 20 20 20 |     CASTOR     |
00000030 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 |                |
00000040 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 33 |               3|
```

## Header Label (HDR*n*)

HDR1 and HDR2 are normally found together at the beginning of a dataset.

### The format for HDR1

| Bytes | Length | Offset | Content |
| ----- | ------ | ------ | ------- |
|   0-3 |     4  |   0x00 | Header label: the characters “HDR1 or EOF1” |
|  4-20 |    17  |   0x04 | File identifier: hexadecimal CTA archive file ID (CASTOR file ID on legacy tapes), aligned to the left. In case of prelabeled tape ‘PRELABEL’ is used instead of file ID. |
| 21-26 |     6  |   0x15 | The volume serial number of the tape. |
| 27-30 |     4  |   0x1B | File section number: CTA writes `0001`. |
| 31-34 |     4  |   0x1F | File sequence number modulo 10000, zero-padded. UHL1 carries the full sequence number. |
| 35-38 |     4  |   0x23 | Generation number: `0001`. |
| 39-40 |     2  |   0x27 | Version number of generation: `00`. |
| 41-46 |     6  |   0x29 | Creation date: local date when the label is filled, in `cyyddd` form (`0` for the current century, two-digit year, day of year 001–366). |
| 47-52 |     6  |   0x2F | Expiration date: CTA fills this with the current local date as well. It is not a retention-policy setting. |
| 53    |     1  |   0x35 | Accessibility: a code indicating the security status of the data set and ‘space’ means no data set access protection. |
| 54-59 |     6  |   0x36 | Block count: `000000` in HDR1; number of data blocks modulo 1000000 in EOF1. |
| 60-72 |    13  |   0x3C | System code of creating system: CTA followed by the software version (CASTOR and its version on legacy tapes). |
| 73-79 |     7  |   0x49 | Reserved |

### The format for HDR2

| Bytes | Length | Offset | Content |
| ----- | ------ | ------ | ------- |
|   0-3 |     4  |   0x00 | Header label: the characters “HDR2 or EOF2” |
|     4 |     1  |   0x04 | Record format: `F`. CTA prelabelling writes HDR1 only, not HDR2. |
|   5-9 |     5  |   0x05 | Block length in bytes (maximum). For a block size of 100000 or more the value is 00000. |
| 10-14 |     5  |   0x0A | Record length in bytes (maximum). For a record size of 100000 or more the value is 00000. |
|    15 |     1  |   0x0F | Tape density: left as a space; not checked by the CTA label verifier. |
| 16-33 |    18  |   0x10 | Reserved |
| 34-35 |     2  |   0x22 | Recording technique: `P ` when compression is enabled for the write session, otherwise spaces. |
| 36-49 |    14  |   0x24 | Reserved (spaces) |
| 50-51 |     2  |   0x32 | AUL identifier: `00`. |
| 52-79 |    28  |   0x34 | Reserved |

### Examples

#### Example for the empty tape with PRELABEL and one HDR1

```hexdump
00000000 56 4f 4c 31 56 35 32 30 30 31 20 20 20 20 20 20 |VOL1V52001      |
00000010 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 |                |
00000020 20 20 20 20 20 72 6f 6f 74 20 20 20 20 20 20 20 |     root       |
00000030 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 |                |
00000040 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 33 |               3|
00000050 48 44 52 31 50 52 45 4c 41 42 45 4c 20 20 20 20 |HDR1PRELABEL    |
00000060 20 20 20 20 20 56 35 32 30 30 31 30 30 30 31 30 |     V5200100010|
00000070 30 30 31 30 30 30 31 30 30 30 31 33 32 33 34 30 |0010001000132340|
00000080 31 33 32 33 34 20 30 30 30 30 30 30 43 41 53 54 |13234 000000CAST|
00000090 4f 52 20 32 2e 31 2e 31 33 20 20 20 20 20 20 20 |OR 2.1.13       |
```

#### Example of HDR1 for the second file on the tape

```hexdump
00000000 48 44 52 31 31 32 41 31 36 30 43 33 38 20 20 20 |HDR112A160C38   |
00000010 20 20 20 20 20 56 35 32 30 30 31 30 30 30 31 30 |     V5200100010|
00000020 30 30 32 30 30 30 31 30 30 30 31 32 30 34 31 30 |0020001000120410|
00000030 31 32 30 34 31 20 30 30 30 30 30 30 43 41 53 54 |12041 000000CAST|
00000040 4f 52 20 32 2e 31 2e 31 32 20 20 20 20 20 20 20 |OR 2.1.12       |
```

#### Example of HDR2 for the first file on the tape

```hexdump
00000000 48 44 52 32 46 30 30 30 30 30 30 30 30 30 30 20 |HDR2F0000000000 |
00000010 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 |                |
00000020 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 |                |
00000030 20 20 30 30 20 20 20 20 20 20 20 20 20 20 20 20 |  00            |
00000040 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 |                |
```

## User Header Label (UHL*n*)

### The format for UHL1

| Bytes | Length | Offset | Content |
| ----- | ------ | ------ | ------- |
|   0-3 |      4 |   0x00 | User header label: the characters “UHL1 or UTL1”. |
|  4-13 |     10 |   0x04 | Actual file sequence number ( ‘0’ padded from left ). |
| 14-23 |     10 |   0x0E | Actual block size ( ‘0’ padded from left ). |
| 24-33 |     10 |   0x18 | Actual record length ( ‘0’ padded from left ). |
| 34-41 |      8 |   0x22 | Site : a part of the domain name uppercase. |
| 42-51 |     10 |   0x2A | Tape mover host name uppercase without domain name. |
| 52-59 |      8 |   0x34 | Drive manufacturer. |
| 60-67 |      8 |   0x3C | Drive model (first 8 bytes from the field PRODUCT IDENTIFICATION in the SCSI INQUIRY replay). |
| 68-79 |     12 |   0x44 | Drive serial number |

### Examples

#### Example for the second file on the tape

```hexdump
00000000 55 48 4c 31 30 30 30 30 30 30 30 30 30 32 30 30 |UHL1000000000200|
00000010 30 30 32 36 32 31 34 34 30 30 30 30 32 36 32 31 |0026214400002621|
00000020 34 34 43 45 52 4e 20 20 20 20 4c 58 43 32 44 45 |44CERN    LXC2DE|
00000030 56 35 44 32 53 54 4b 20 20 20 20 20 54 31 30 30 |V5D2STK     T100|
00000040 30 30 42 20 58 59 5a 5a 59 5f 42 31 20 20 20 20 |00B XYZZY_B1    |
```

## Data Records

After the header group and its tape mark, file data is written in blocks, followed by a tape mark and the trailer group. The current tape-write path uses 256 KiB data blocks; the final block may be shorter. UHL1 records the block size used, which the reader uses when retrieving the file.

## End of File (EOF*n*)

EOF1 and EOF2 are normally found together at the end of a dataset.

CTA writes EOF1 and EOF2 followed by UTL1. It does not write EOV continuation labels or split a file across tape volumes; EOV belongs to the general label-standard background described in [Tape Labels](labels.md).

## User Trailer Label (UTL*n*)

The format for UTL1 is the same as UHL1 (see above).

## Checksums

When a file is written to tape, an [Adler32](http://www.zlib.net/manual.html#Checksum) checksum is computed on the file.
The file checksum is checked against the expected checksum and stored in the Catalogue, not in the CTA label records.

Logical block protection (LBP) is separate from the file checksum. When CRC32C LBP is enabled, CTA's drive interface appends a four-byte CRC32C value to each block before passing it to the drive. On reading, the interface verifies the protection value and removes those four bytes before returning the block contents.

These protection bytes are additional to the block's contents, not part of the original file payload or the label fields. They protect individual blocks; the Adler-32 checksum in the Catalogue covers the file as a whole. For example, a protected 80-byte label is passed to the drive with four additional LBP bytes, while CTA's label reader receives the original 80 bytes.

The LBP method is recorded in VOL1. The read/write sessions support CRC32C or no LBP; they reject the Reed–Solomon method.
