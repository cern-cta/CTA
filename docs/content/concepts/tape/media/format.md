# Supported tape formats

CTA reads and writes the CTA tape format, which is based on that of CASTOR.

## CTA/CASTOR tape format

CTA uses the same AUL file format as CASTOR.
CASTOR used several file formats over time, but by 2013, only the AUL format was in use.
This format is based on [ANSI INCITS 27-1987](http://webstore.ansi.org/RecordDetail.aspx?sku=INCITS+27-1987%5bS2008%5d) and is described in detail on the [Tape Labels, ANSI and IBM](http://it-dep-fio-ds.web.cern.ch/it-dep-fio-ds/Documentation/tapedrive/labels.html) web page (last updated in 2008).

The AUL format has the following descriptors:

* Volume Label (VOL1)
* Header Blocks: Headers (HDR1, HDR2) and User Header Labels (UHL1)
* Trailer Blocks: User Trailer Labels UTL1
  * The UHLs and UTLs are defined in ANSI X 3.27. The general description of the ANSI fields was documented in IBM’s
z/OS documentation.

Each of these descriptor labels is contained in an 80-byte tape block of ASCII text.
Empty bytes are stored as spaces (0x20).
The label descriptor must begin with the 4-byte identifier.
Labels are terminated by a file mark: Tape Mark (TM) or End of File (EOF)..

| VOL1 | HDR1 \| HDR2 \| UHL1 \| TM \| DATA \| TM \| EOF1 \| EOF2 \| UTL1 \| TM |
| ---- | ---------------------------------------------------------------------- |
|      | <-------------------------- One data file ---------------------------------------------------> |

Volumes that have just been initialised contain no data records, just a single ‘header label group’:

| VOL1 | HDR1(PRELABEL) \| TM |
| ---- | -------------------- |
|      |                      |

### Volume Label (VOL*n*)

The very first label record on a labelled volume is VOL1.
If this label is incorrect, you will not advance at all.

#### The structure of the volume label (VOL1)

| Bytes | Length | Offset | Content |
| ----- | ------ | ------ | ------- |
|  0-3  |    4   |  0x00  | Volume label indicator: the characters VOL1 |
|  4-9  |    6   |  0x04  | Volume serial number (VSN) (e.g., “AB1234”) |
|   10  |    1   |  0x0A  | Accessibility (left as empty space)         |
| 11-23 |    13  |  0x0B  | Reserved (spaces)                           |
| 24-36 |    13  |  0x18  | Implementation identifier (left as empty spaces) |
| 37-50 |    14  |  0x25  | Owner identifier (the string “CASTOR” or STAGESUPERUSER name, padded with spaces) |
| 51-78 |    28  |  0x33  | Reserved (spaces) |
|   79  |    1   | 0x4F   | Label standard level (1, 3 and 4 are listed as valid in IBM’s documentation. CASTOR uses ASCII ‘3’) |

#### Examples

##### An example of the beginning of the tape

``` hexdump
00000000 56 4f 4c 31 56 35 32 30 30 31 20 20 20 20 20 20 |VOL1V52001     |
00000010 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 |               |
00000020 20 20 20 20 20 43 41 53 54 4f 52 20 20 20 20 20 |     CASTOR    |
00000030 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 |               |
00000040 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 33 |              3|
```

### Header Label (HDR*n*)

HDR1 and HDR2 are normally found together at the beginning of a dataset.

#### The format for HDR1

| Bytes | Length | Offset | Content |
| ----- | ------ | ------ | ------- |
|   0-3 |     4  |   0x00 | Header label: the characters “HDR1 or EOF1” |
|  4-20 |    17  |   0x04 | File identifier: hexadecimal CASTOR NS file ID. nsgetpath -x can be used to find the CASTOR full path name. Aligned to left. In case of prelabeled tape ‘PRELABEL’ is used instead of file ID. |
| 21-26 |     6  |   0x15 | The volume serial number of the tape. |
| 27-30 |     4  |   0x1B | File section number: a number (0001 to 9999) that indicates the order of the volume within the multivolume aggregate. This number is always 0001 for a single volume data set. |
| 31-34 |     4  |   0x1F | File sequence number: a number that indicates the relative position of the data set within a multiple data set group (aggregate). CASTOR uses modulus for fseq by 10000 |
| 35-38 |     4  |   0x23 | Generation number: ‘0001’ in CASTOR. |
| 39-40 |     2  |   0x27 | Version number of generation: ‘00’ in CASTOR. |
| 41-46 |     6  |   0x29 | Creation date: Date when allocation begins for creating the data set. The date format is cyyddd, where: c = century (blank=19; 0=20; 1=21; etc.) yy = year (00-99) ddd = day (001-366) |
| 47-52 |     6  |   0x2F | Expiration date: year and day of the year when the data set may be scratched or overwritten. The data is shown in the format cyyddd. It is always advisable to set the expiration date when a volume is being initialised (‘prelabeled’) to be a date before the current date, so that writing to the tape is immediately possible. |
| 53    |     1  |   0x35 | Accessibility: a code indicating the security status of the data set and ‘space’ means no data set access protection. |
| 54-60 |     6  |   0x36 | Block count: This field in the trailer label shows the number of data blocks in the data set on the current volume. This field in the header label is always ‘000000’.
| 60-72 |    13  |   0x3C | System code of creating system: a unique code that identifies the system. CASTOR with CASTOR BASEVERSION number string. |
| 73-79 |     7  |   0x49 | Reserved |

#### The format for HDR2

| Bytes | Length | Offset | Content |
| ----- | ------ | ------ | ------- |
|   0-3 |     4  |   0x00 | Header label: the characters “HDR2 or EOF2” |
|     4 |     1  |   0x04 | Record format. An alphabetic character that indicates the format of the records in the associated data set. For the AUL it could be only: F - fixed length (U - was used for HDR2 for prelabeled tapes) |
|   5-9 |     5  |   0x05 | Block length in bytes (maximum). For the block size greater than 100000 the value is 00000. |
| 10-14 |     5  |   0x0A | Record length in bytes (maximum). For the record size greater than 100000 the value is 00000. |
|    15 |     1  |   0x0F | Tape density. Depends on the tape density values are following: ‘2’ for D800, ‘3’ for D1600, ‘4’ for D6250 |
| 16-33 |    18  |   0x10 | Reserved |
|    34 |     2  |   0x22 | Tape recording technique. The only technique available for 9-track tape is odd parity with no translation. For a magnetic tape subsystem with Improved Data Recording Capability, the values are: ‘P ’- Record data in compacted format, ‘ ’ - Record data in standard uncompacted format. For CASTOR is is ‘P’ if the drive configured to use compression (i.e. xxxGC) |
| 35-49 |    14  |   0x24 | Reserved
| 50-51 |     2  |   0x32 | Buffer offset ‘00’ for AL and AUL tapes
| 52-79 |    28  |   0x34 | Reserved |

#### Examples

##### Example for the empty tape with PRELABEL and one HDR1

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

##### Example of HDR1 for the second file on the tape

```hexdump
00000000 48 44 52 31 31 32 41 31 36 30 43 33 38 20 20 20 |HDR112A160C38   |
00000010 20 20 20 20 20 56 35 32 30 30 31 30 30 30 31 30 |     V5200100010|
00000020 30 30 32 30 30 30 31 30 30 30 31 32 30 34 31 30 |0020001000120410|
00000030 31 32 30 34 31 20 30 30 30 30 30 30 43 41 53 54 |12041 000000CAST|
00000040 4f 52 20 32 2e 31 2e 31 32 20 20 20 20 20 20 20 |OR 2.1.12       |
```

##### Example of HDR2 for the first file on the tape

```hexdump
00000000 48 44 52 32 46 30 30 30 30 30 30 30 30 30 30 20 |HDR2F0000000000 |
00000010 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 |                |
00000010 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 |                |
00000030 20 20 30 30 20 20 20 20 20 20 20 20 20 20 20 20 |  00            |
00000040 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 |                |
```

### User Header Label (UHL*n*)

#### The format for UHL1

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

#### Examples

##### Example for the second file on the tape

```hexdump
00000000 55 48 4c 31 30 30 30 30 30 30 30 30 30 32 30 30 |UHL1000000000200|
00000010 30 30 32 36 32 31 34 34 30 30 30 30 32 36 32 31 |0026214400002621|
00000020 34 34 43 45 52 4e 20 20 20 20 4c 58 43 32 44 45 |44CERN    LXC2DE|
00000030 56 35 44 32 53 54 4b 20 20 20 20 20 54 31 30 30 |V5D2STK     T100|
00000040 30 30 42 20 58 59 5a 5a 59 5f 42 31 20 20 20 20 |00B XYZZY_B1    |
```

### Data Records

After a ‘header label group’, data records follow of any length and in any number.
Eventually, an EOF will appear and then a ‘trailer label group’ is expected.

The data block size is configurable but in practice a block size of 256 KiB has been used everywhere.

### End of File (EOF*n*)

EOF1 and EOF2 are normally found together at the end of a dataset.

Note that an End of Volume (EOV*n*) label will appear instead of EOF*n* if this is the final label group on the volume, but the dataset continues on another volume.
EOV1 and EOV2 are only expected together and at the end of a volume.

### User Trailer Label (UTL*n*)

The format for UTL1 is the same as UHL1 (see above).

### Checksums

When a file is written to tape, an [Adler32](http://www.zlib.net/manual.html#Checksum) checksum is computed on the file.
The main advantages of Adler32 are that it is faster to compute than CRC32 or MD5, and it is distributive when computing the checksum for a multi-block file.
This checksum is not stored on the tape; it is stored as metadata in the Catalogue.

!!! note
    The tape drives also compute a CRC32 checksum on each block, which is checked in firmware.
    This checksum is not seen by the software.

