<!-- Adapted from CERN's historical Tape Labels, ANSI and IBM reference:
https://it-dep-fio-ds.web.cern.ch/Documentation/tapedrive/labels.html -->

# Tape Labels, ANSI and IBM

## Purpose and standards

Tape labels are records written on the tape that identify the volume and describe its files. They are distinct from the barcode attached to the cartridge. This page explains the label families and preserves useful field layouts from the historical CERN reference.

[ISO/IEC 1001:2012 — File structure and labelling of magnetic tapes for information interchange](https://www.iso.org/standard/60220.html) describes the general structure and labelling standard. The historical reference also uses the ANSI X3.27 designation. [IBM's label documentation](https://www.ibm.com/docs/en/zos/3.2.0?topic=labels-label-definitions-organization) explains the IBM label families. These are background references, not complete specifications of the CTA format.

For the layout written by CTA, see [CTA Tape Format](format.md). For preparing and labelling cartridges, see [Media Initialisation](../../../ops/administration/media-initialisation.md).

## Tape terminology

- **Tape record (block):** a unit of bytes written to or read from tape. A record can contain file data or a label; the drive preserves record boundaries.
- **Label:** a record whose contents describe a volume or file. Labels such as `VOL1`, `HDR2`, and `EOF1` contain 80 bytes of structured text that CTA interprets.
- **Tape mark (file mark):** a special separator written and recognised by the drive, not a record containing text or file data. Reads detect it as a boundary, and the drive can position forwards or backwards by counting tape marks.
- **Label group:** consecutive label records, such as `HDR1`, `HDR2`, and `UHL1`, describing the same file. A tape mark separates this header group from the file data.

Despite their names, `EOF1` and `EOF2` are label records, not tape marks. CTA writes a tape mark after the file data, then these end-of-file labels as part of the trailer group, followed by another tape mark. One archived file therefore spans several tape-mark-separated groups.

## Label sequence

Labels are 80-byte records. The first four characters identify the label type. CTA uses ASCII; IBM standard labels may use EBCDIC. Label groups are separated from file data by tape marks, which are distinct from records named `EOF1` or `EOF2`.

| Label | Purpose | Use in the CTA format |
| --- | --- | --- |
| VOL1 | Identifies the tape volume | At the beginning of the tape |
| HDR1 / HDR2 | Describe the following file | Before each file |
| UHL1 | Additional file and writer metadata | After the file headers |
| EOF1 / EOF2 | Describe the completed file | After each file |
| UTL1 | Additional trailer metadata | After the file trailers |
| EOV1 / EOV2 | Describe a file continued on another volume | Standard variant; CTA does not write continuation labels |

A tape containing a file therefore begins with:

```text
VOL1 → HDR1 → HDR2 → UHL1 → tape mark → file data → tape mark
     → EOF1 → EOF2 → UTL1 → tape mark
```

Subsequent files repeat the sequence without VOL1. Newly labelled tapes contain no file data; their initial label sequence is described in [CTA Tape Format](format.md).

## Label fields

**Byte positions below are one-based and inclusive.** The CTA Tape Format page uses zero-based byte offsets. The tables below describe inherited ANSI/IBM layouts; application-specific fields and examples are not universal across formats. Generic examples such as `U` record format or blank generation-version fields are not CTA writer defaults; use [CTA Tape Format](format.md) for those values.

In particular, CTA writes `CTA` as VOL1's owner, `3` as its label-standard level, and uses bytes 78–79 of its historically reserved area for the logical block protection method. Its archive file ID and catalogue metadata provide the CTA interpretation of file labels.

### VOL1 label

| Bytes | Length | Example |  Significance to user                                               |
| ----: | -----: | ------: | ------------------------------------------------------------------- |
| 1-3   | 3      | VOL     |  Always, first label record of volume                               |
| 4     | 1      | 1       |  Always, first label record of volume                               |
| 5-10  | 6      | AB1234  |  The VID/VOLSER/Volume Serial Number/VSN                            |
| 11    |  1     | space   |  Accessibility. Volume usable.                                      |
| 12-37 | 26     | spaces  |  Reserved                                                           |
| 38-51 | 14     | owner   |  Usually spaces                                                     |
| 52-79 | 28     | space   |  Reserved                                                           |
| 80    |  1     | 1       |  Always 1 for ANSI, ASCII 31 hex <br> Usually 3 for DEC VMS volumes |

### HDR1, EOF1 or EOV1 label

| Bytes | Length | Example   | Significance to user                                              |
| ----: | -----: | --------: | ----------------------------------------------------------------- |
| 1-3   | 3      | HDR       | Header label (EOF or EOV possible)                                |
| 4     | 1      | 1         | Header label number, 1.                                           |
| 5-21  | 17     | file ID   | File identifier; CTA writes its archive file ID in hexadecimal |
| 22-27 | 6      | AB1234    | Set identifier (VSN, 1st volume)                                  |
| 28-31 | 4      | 0001      | File section number                                               |
| 32-35 | 4      | 0001      | Four-digit file sequence field; CTA stores the sequence modulo 10000                                   |
| 36-39 | 4      | 0001      | Generation number                                                 |
| 40-41 | 2      | spaces    | Version number of generation                                      |
| 42-47 | 6      | cyyddd    | Creation date, e.g. '000017'                                      |
| 48-53 | 6      | cyyddd    | Expiration date, e.g. '000017'                                    |
| 54    | 1      | space     | Accessibility                                                     |
| 55-60 | 6      | 000000    | Block count                                                       |
| 61-73 | 13     | CERNVM    | System code of creating system <br> IBMnnnnhhmmss sometimes for IBM <br> DECFILE11A or similar for DEC |
| 74-80 | 7      | spaces    |  Reserved                                                         |

The date form includes a century code, `c`, which is blank for 1900–1999 and `0` for 2000–2099. These label fields do not replace CTA catalogue policies.

### HDR2, EOF2 or EOV2 label

| Bytes | Length | Example | Significance to user                 |
| ----: | -----: | ------: | ------------------------------------ |
| 1-3   | 3      | HDR     | Header label (EOF or EOV possible).  |
| 4     | 1      | 2       | Header label number, 2.              |
| 5     | 1      | U       | Record format. F, U or V (IBM only). |
| 6-10  | 5      | 32000   | Block length in bytes (maximum).     |
| 11-15 | 5      | 32000   | Record length in bytes (maximum).    |
| 16-80 | 65     | spaces  |                                      |
| 16    | 1      | 5       | Recording density (IBM). 0-5.        |
| 35-36 | 2      | P       | Compressed data follows (3490 etc.). |

### HDRn, EOFn or EOVn label

| Bytes | Length | Example | Significance to user                 |
| ----: | -----: | ------: | ------------------------------------ |
| 1-3   | 3      | HDR     | Header label (EOF or EOV possible).  |
| 4     | 1      | 3       | Header label number, n in range 3-9. |
| 5-80  | 76     | spaces  | Not used by system.                  |

### UHLn, UTLn label

User labels carry application-specific metadata. CTA writes UHL1 and UTL1; the historical extensions below are not part of its written format.

### UHL1 and UTL1

| Bytes | Length | Example      | Significance to user             |
| ----: | -----: | -----------: | -------------------------------- |
| 1-3   | 3      | UHL          | User Header label (UTL possible) |
| 4     | 1      | 1            | Header label number              |
| 5-14  | 10     | 0000012345   | Actual file sequence number     |
| 15-24 | 10     | 0000262144   | Actual block size in bytes       |
| 25-34 | 10     | 0000262144   | Actual record length             |
| 35-42 | 8      | CERN         | Site                             |
| 43-52 | 10     | TPSRV201     | Tape mover hostname              |
| 53-60 | 8      | STK          | Drive manufacturer               |
| 61-68 | 8      | T9940B       | Drive model                      |
| 69-80 | 12     | 456000001642 | Drive serial number              |

## Character encodings

These codes help distinguish ASCII from EBCDIC labels when examining a tape dump. The punctuation rows list ASCII only; EBCDIC punctuation depends on the code page.

| Character   | EBCDIC code   |  ASCII code  |
| ----------- | ------------- | ------------ |
| symbol      |  Dec  Hex     | Dec  Hex     |
| A           |  193   C1     |  65   41     |
| B           |  194   C2     |  66   42     |
| C           |  195   C3     |  67   43     |
| D           |  196   C4     |  68   44     |
| E           |  197   C5     |  69   45     |
| F           |  198   C6     |  70   46     |
| G           |  199   C7     |  71   47     |
| H           |  200   C8     |  72   48     |
| I           |  201   C9 hex |  73   49 hex |
| J           |  209   D1     |  74   4A     |
| K           |  210   D2     |  75   4B     |
| L           |  211   D3     |  76   4C     |
| M           |  212   D4     |  77   4D     |
| N           |  213   D5     |  78   4E     |
| O           |  214   D6     |  79   4F     |
| P           |  215   D7     |  80   50     |
| Q           |  216   D8     |  81   51     |
| R           |  217   D9 hex |  82   52 hex |
| S           |  226   E2     |  83   53     |
| T           |  227   E3     |  84   54     |
| U           |  228   E4     |  85   55     |
| V           |  229   E5     |  86   56     |
| W           |  230   E6     |  87   57     |
| X           |  231   E7     |  88   58     |
| Y           |  232   E8     |  89   59     |
| Z           |  233   E9 hex |  90   5A hex |
| 0           |  240   F0     |  48   30     |
| 1           |  241   F1     |  49   31     |
| 2           |  242   F2     |  50   32     |
| 3           |  243   F3     |  51   33     |
| 4           |  244   F4     |  52   34     |
| 5           |  245   F5     |  53   35     |
| 6           |  246   F6     |  54   36     |
| 7           |  247   F7     |  55   37     |
| 8           |  248   F8     |  56   38     |
| 9           |  249   F9 hex |  57   39 hex |
| space       |  64    40     |  32   20     |
| !           | — |  33    21     |
| "           | — |  34    22     |
| % percent   | — |  37    25     |
| & ampersand | — | 38    26      |
| '           | — | 39    27      |
| (           | — | 40    28      |
| )           | — | 41    29      |
| * asterisk  | — | 42    2A      |
| + plus      | — | 43    2B      |
| , comma     | — | 44    2C      |
| - minus     | — | 45    2D      |
| . fullstop  | — | 46    2E      |
| /           | — | 47    2F      |
| : colon     | — | 58    3A      |
| ; semicolon | — | 59    3B      |
| < less      | — | 60    3C      |
| = equal     | — | 61    3D      |
| > greater   | — | 62    3E      |
| ? question  | — | 63    3F hex  |

## Historical extensions

The following UHL2–4 and UTL2–4 layouts were proposed for CASTOR. They are preserved for historical interpretation and are not labels written in the CTA format. Other tape formats may use the same label names with different layouts.

??? info "Proposed user-label layouts"

    ### UHL2 and UTL2

    | Bytes | Length | Example              | Significance to user             |
    | ----: | -----: | -------------------: | -------------------------------- |
    | 1-3   | 3      | UHL                  | User Header label (UTL possible) |
    | 4     | 1      | 2                    | Header label number              |
    | 5-24  | 20     | 00000000000000376975 | Bit file ID (64 bits)            |
    | 25-34 | 10     | CASTORNS1            | Name Server hostname             |
    | 35-38 | 4      | 0644                 | Absolute mode                    |
    | 39-48 | 10     | 0000000395           | Uid                              |
    | 49-58 | 10     | 0000001028           | Gid                              |
    | 59-78 | 20     | 00000000010031553895 | File size in bytes (64 bits)     |

    ### UHL3 and UTL3

    | Bytes | Length | Example             | Significance to user                              |
    | ----: | -----: | ------------------: | ------------------------------------------------- |
    | 1-3   | 3      | UHL                 | User Header label (UTL possible)                  |
    | 4     | 1      | 3                   | Header label number                               |
    | 5-18  | 14     |                     | User name                                         |
    | 19-26 | 8      |                     | Experiment/Project name                           |
    | 27-28 | 2      |                     | Checksum algorithm (AD for adler32, CS for cksum) |
    | 29-38 | 10     |                     | File checksum (32 bits)                           |
    | 39-57 | 20     | 2001/04/04 08:51:30 | Last modification (UTC)                           |

    ### UHL4 and UTL4

    | Bytes | Length | Example              | Significance to user                              |
    | ----: | -----: | -------------------: | ------------------------------------------------- |
    | 1-3   | 3      | UHL                  | User Header label (UTL possible)                  |
    | 4     | 1      | 4                    | Header label number                               |
    | 5-9   | 5      | 00001                | Copy number                                       |
    | 10-14 | 5      | 00001                | Segment number                                    |
    | 15-34 | 20     | 00000000010031553895 | Segment size in bytes (64 bits)                   |
    | 35-36 | 2      |                      | Checksum algorithm (AD for adler32, CS for cksum) |
    | 37-46 | 10     |                      | Segment checksum (32 bits)                        |
    | 47-65 | 20     | 2001/04/04 08:51:30  | Tape write timestamp (UTC)                        |
    | 66-75 | 10     | 0000002342           | Number of blocks                                  |
