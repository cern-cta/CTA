<!-- Note: The copyright url at the bottom of the page was dead. A copy from the year of publishing can be found in the WayBackMachine https://web.archive.org/web/20071224042346/http://cern-copyright.web.cern.ch/cern-copyright/ -->

<!-- This is a transcription of https://it-dep-fio-ds.web.cern.ch/Documentation/tapedrive/labels.html, before it was lost. -->

# Tape Labels, ANSI and IBM

!!! tip
    Further information about tape labels and file structures may be found in ISO 1001.

## Standard Labels

All users of tape media are **strongly advised to use magnetic labels**.
The format of these labels is defined by ANSI Standard X 3.27, and major computer suppliers such as IBM provide relevant documentation.
One such manual published by IBM is:

> Using Magnetic Tape Labels and File Structure <br> SC26-4565 (1991)

However, not every system will handle them correctly: 
This is particularly true of Unix systems, which are generally very poor in this area. 
You would be sensible if you considered installing the portable tape daemon and related software developed at CERN for Linux, which provides good support for label handling.

For systems which do handle labels, these labels can offer a greater level of security in data handling, as there are several fields that are checked by the operating system before control of the volume is handed to the user. 
Once he has control, he can still destroy the volume by inadvertently rewinding and then writing, but many common sources of problems are avoided. 
What then are these labels?

## Generalities

CERN's Hierarchical Storage System proposes to make use of the User Header Labels (UHLs) and User Trailer Labels (UTLs) permitted by the ANSI X 3.27 standard. 
This allows it to overcome some of the deficiencies suffered by the (very) old ANSI standard. 
See [here](http://castor.web.cern.ch/castor/DOCUMENTATION/TAPE/) for details.

All labels are 80 byte records of 8 bit bytes. All labels come in label groups, terminated by a file mark (tape mark, eof). 
All but the first label group on a volume are also preceded by a file mark (tape mark, eof). 
All labels you are likely to come across are written as text strings of ASCII or more rarely nowadays (IBM) EBCDIC characters.

Label records must start with one of the following sets of 4 bytes:

VOL1 for the very first label record on a labelled volume.
If this label is incorrect, you will not advance at all.

HDRn where n is one of 1-9, the 'header label'. 
HDR1 and HDR2 are normally found together at the beginning of a dataset. 
DEC systems such as VMS often use HDR3, and higher, also. They should appear in increasing sequence of n.

UHLn where n is one of 1-9, the 'user header label'. 
This label is not accepted in practice by all systems, but it should be 'tolerated'. 
It will be up to you to process any information in it They should appear in increasing sequence of n. 
IBM label processing permits only UHL1-8, while ANSI labels may in fact use any ISI/ANSI/FIPS hexadecimal character for 'n'. 
This character set consists of the following:

* `A-Z`, upper case alphabetic
* `0-9`, numeric digits
* `space` (32 decimal in ASCII, 40 hexadecimal in EBCDIC)
* `! " % & ' ( ) * + , - . / : ; < = > or ?` ('special' characters)

After a 'header label group', such as VOL1/HDR1/HDR2/eof, data records follow and in principle can be of any length and in any number. 
They are normally more than 80 bytes and until recently were rarely more than 32760 bytes in length.
Beware!
Many old systems will still object to a block length greater than can be held in
16 bits, that is 32765 bytes. 
Eventually, an eof will appear and then a 'trailer label group' is expected.
Some older tapes may just show one or more eofs, and end. A valid trailer group should however contain the following.

EOFn where n is one of 1-9, the 'end of file label'. EOF1 and EOF2 are normally found together at the end of a dataset.
They should appear in increasing sequence of n.

UTLn where n is one of 1-9, the 'user trailer label'. 
This label is tolerated, but it will be up to you to process any information in it.
They should appear in increasing sequence of n.
IBM label processing permits only UTL1-8, while ANSI labels may in fact use any ISI/ANSI/FIPS hexadecimal character for 'n', as in the case of 'UHL' labels.

Note that EOVn where n is one of 1-9, the 'end of volume label', will appear instead of EOFn if this is the final label group on the volume, but the dataset continues on another volume.
EOV1 and EOV2 are only expected together and at the end of a volume.
They should appear in increasing sequence of n.

## Volumes that have just been initialised

Such a volume will contain no data records, and normally a single 'header label group'.
Most operating systems will use the ANSI label scheme, if any, and record the first 4 characters as
`564F4C31` in hexadecimal (`VOL1` in ASCII).
Older IBM systems will typically write this in EBCDIC code, which for the VOL1 label record will start with the characters `E5D6D3F1` (`VOL1` in EBCDIC).

The normal sequence you will see is `VOL1/HDR1/eof/eof`, in either ASCII or EBCDIC codes.
Volumes initialised for use with DEC systems may show a `VOL1/HDR1/HDR2/eof/eof` sequence, and will usually be in ASCII code.

Occasionally, you may see a volume with a sequence VOL1 in EBCDIC followed by no other label or file mark, padded with 0s.
This is the result of the IBM 'WVOL1' initialisation command, and can cause problems for many tape label handling systems.
Once you are sure this is what you are looking at, and that there is no useful data on the tape, you may decide to **re-initialise the volume**.

## ANSI X 3.27 standard has serious limitations

This standard and the ISO 1001 standard are very old.
They were first used in 1978-1979, when 7-track and 9-track tapes containing about 160 MBytes were the most common media.

Present-day tapes such as the Redwood 50 GByte volumes use by default at CERN a block size of 256K (K=1024) or as near to this as possible.
The number of files written to a tape can easily reach 100,000 or more.

The restriction in the standard description to volume serial numbers of 6 characters, to block lengths of at most 99999 bytes, and to at most 9999 files, is now a great nuisance. 
The 'recommended' method of handling this problem is to specify 0 bytes, and provide the necessary information to the operating system of the host in some other way.

A further difficulty is caused by the lack of any specification of a standard external label. 
Thus we have OCR stickers, code39 barcode stickers, and now a variety of 'media identification' codes.
The normal use of code39 should be to present an 8-character code for a volume serial number (the external sticker is usually referred to as the Volume IDentifier or VID at CERN), of the form `*AB1234*`. 
The `*` characters are the standard start/stop codes.
Normally, the code can be read in either direction.
However, IBM robotic libraries expect one particular print direction.
STK silo libraries use `$` as the start/stop code and normal code39 equipment cannot decode these labels.
Scanners capable of decoding the STK code and normal code39 are available at CERN. These are the
Symbol Technologies LS 400xi and the [Worthington Data Solutions LZ 200](http://www.barcodehq.com)

The media identification normally comes pre-attached to media.
On older media (such as STK REdwood) this was a separate 'single character' label.
More modern media incorporates the media identification as an extension to the VID.
Current commonly used media identifiers are:

| Vendor | Model    | Capacity              | Identifier       |
| ------ | -------- | --------------------- | ---------------- |
| IBM    | 3480     | (200 MByte, 18 track) | none, absent     |
| IBM    | 3490     | (800 MByte, 36 track) | E                |
| IBM    | 3590     | (10 GByte, 128 track) | J                |
| IBM    | 3590     | (cleaning tape)       | C                |
| IBM    | 3592 J1A | (300/500 GB)          | JA (symbols 7-8) |
| IBM    | 3592 E05 | (500/700 GB)          | JB (symbols 7-8) |
| STK    | Redwood  | (10 GByte helical)    | A                |
| STK    | Redwood  | (25 GByte helical)    | B                |
| STK    | Redwood  | (50 GByte helical)    | C                |
| STK    | Redwood  | (cleaning tape)       | D                |
| STK    | DLT7000  | (35 GByte)            | D  (symbol 7)    |
| STK    | T10000A  | (500 GB)              | T1 (symbols 7-8) |
| LTO    | 1        |   (100 GB)            | L1 (symbols 7-8) |
| LTO    | 2        |   (200 GB)            | L2 (symbols 7-8) |
| LTO    | 3        |   (400 GB)            | L3 (symbols 7-8) |
| LTO    | 4        |   (800 GB)            | L4 (symbols 7-8) | 


## EBCDIC and ASCII character codes you may see in labels

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
| L           |  211   C3     |  76   4C     |
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
| !           |  33    21     | | 
| "           |  34    22     | |
| % percent   |  37    25     | |
| & ampersand | 38    26      | |
| '           | 39    27      | |
| (           | 40    28      | |
| )           | 41    29      | |
| * asterisk  | 42    2A      | |
| + plus      | 43    2B      | |
| , comma     | 44    2C      | |
| - minus     | 45    2D      | |
| . fullstop  | 46    2E      | |
| /           | 47    2F      | |
| : colon     | 58    3A      | |
| ; semicolon | 59    3B      | |
| < less      | 60    3C      | |
| = equal     | 61    3D      | |
| > greater   | 62    3E      | |
| ? question  | 63    3F hex  | |

## Fields you may see in labels and their meanings</H3>

The layout of data in the 80 bytes of the standard VOL1, HDRn, EOF and EOVn labels is standardised and does not vary greatly between the ANSI version of a label or the IBM version. 
The most significant difference is the character coding, ASCII being most common nowadays but EBCDIC codes still appearing from time to time, especially on older tapes.

!!! info
    CERN presently uses the VOL1/HDR1/HDR2/UHL1, HDR1/HDR2/UHL1, and EOF1/EOF2/UTL1 combinations.
    Plans to use UHL2, UTL2 have never been implemented.

### VOL1 label
| Bytes | Length | Example |  Significance to user                                               |
| ----: | -----: | ------: | ------------------------------------------------------------------- |
| 1-3   | 3      | VOL     |  Always, first label record of volume                               |
| 1     | 1      | 1       |  Always, first label record of volume                               |
| 6-10  | 6      | AB1234  |  The VID/VOLSER/Volume Serial Number/VSN                            |
| 11-80 | 70     | spaces  |  IBM uses EBCDIC 40 hex                                             |
| 11    |  1     | space   |  Accessibility. Volume usable.                                      |
| 12-37 | 26     | spaces  |  Reserved                                                           |
| 38-51 | 14     | owner   |  Usually spaces                                                     |
| 52-79 | 28     | space   |  Reserved                                                           |
| 80    |  1     | 1       |  Always 1 for ANSI, ASCII 31 hex <br> Usually 3 for DEC VMS volumes |

### HDR1, EOF1 or EOV1 label

| Bytes | Length | Example   | Significance to user                                              |
| ----: | -----: | --------: | ----------------------------------------------------------------- |
| 1-3   | 3      | HDR       | Header label (EOF or EOV possible)                                |
| 1     | 1      | 1         | Header label number, 1.                                           |
| 5-21  | 17     | MYDATA    | File identifier (non-CASTOR or CASTOR Version 1.x tapes)          |
| 5-21  | 17     | bitfileID | Hexadecimal CASTOR bitfileID (CASTOR Version 2.1 or higher tapes) |
| 22-27 | 6      | AB1234    | Set identifier (VSN, 1st volume)                                  |
| 28-31 | 4      | 0001      | File section number                                               |
| 32-35 | 4      | 0001      | File sequence number, 0001-9999                                   |
| 36-39 | 4      | 0001      | Generation number                                                 |
| 40-41 | 2      | spaces    | Version number of generation                                      |
| 42-47 | 6      | cyyddd    | Creation date, e.g. '000017'                                      |
| 48-53 | 6      | cyyddd    | Expiration date, e.g. '000017'                                    |
| 54    | 1      | space     | Accessibility                                                     |
| 55-60 | 6      | 000000    | Block count                                                       |
| 61-73 | 13     | CERNVM    | System code of creating system <br> IBMnnnnhhmmss sometimes for IBM <br> DECFILE11A or similar for DEC |
| 74-80 | 7      | spaces    |  Reserved                                                         |

The CASTOR bitfileID (HDR1 5-21 above, in Hexadecimal) can be used to directly find the CASTOR file full path name:

nsGetPath castorns bitfileID-10   (NB decimal value of the bitfileID!)

Note that the date form includes the 'century' code, 'c', which was blank for the years 1900-1999 and is '0' for the years 2000-2099.
It is always advisable to set the **expiration date** when a volume is being initialised ('prelabelled') to be a date before the current date, so that writing to the tape is immediately possible. 

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

For the original document proposing to use the UHL and UTL within CASTOR at CERN, see [here](http://castor.web.cern.ch/castor/DOCUMENTATION/TAPE/uhl_proposal_v5.doc). 

### UHL1 and UTL1

| Bytes | Length | Example      | Significance to user             |
| ----: | -----: | -----------: | -------------------------------- |
| 1-3   | 3      | UHL          | User Header label (UTL possible) |
| 4     | 1      | 1            | Header label number              |
| 5-14  | 10     | 000012345    | Actual files sequence number     |
| 25-34 | 10     | 000262144    | Actual record length             |
| 35-42 | 8      | CERN         | Site                             |
| 43-52 | 10     | TPSRV201     | Tape mover hostname              |
| 53-60 | 8      | STK          | Drive manufacturer               |
| 61-68 | 8      | T9940B       | Drive model                      |
| 69-80 | 12     | 456000001642 | Drive serial number              |

### UHL2 and UTL2

!!! warning
    Not in use at CERN

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

!!! warning 
    Not in use at CERN

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

!!! warning
    Not in use at CERN

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

