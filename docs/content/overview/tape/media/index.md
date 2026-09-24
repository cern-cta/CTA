# Tape Media

*Tape media* refers to the magnetic tape data storage device itself, which in the present generation of tape technology takes the shape of a *cartridge* filled with a magnetic tape band on which the data is written.

## Cartridge

A cartridge is stored inside of a tape library, and read/written by a tape drive.
Generally, it contains about 1km worth of 12.65mm wide magnetic tape band.

Data is written on this tape in a *boustrophedon* manner:
Bits are written on to a set of parallel *tracks*, which are organized in *wraps* spanning from one end of the tape to the other.
Where one wrap ends, another begins, going in the opposite direction.

TODO: Illustration

## Cartridge Formats

At present, two media types are actively developed and produced:

1. LTO
2. IBM3592 (also referred to as Enterprise)

Both of these are supported by CTA.

### Identifiers: VID/VOLSER

A tape cartridge is identified by a 6-character ( `[A-Z0-9]{6}` ) *Volume IDentifier (VID)*, or *VOLSER (VOLume SERial)*, - the barcode of the cartridge.
The VID of each cartridge must be unique, ideally at the level of the storage system, but definitely within the cartridge's tape library.

Usually, a range of VIDs is specified by the administrator at the time of media purchase and printed on stickers that are put on the cartridges by the supplier.
There is no particular rule as to which range is assigned to which kind of cartridges.
The only consideration that has to be taken into account is when a physical library is partitioned.
In that case (for example for IBM tape libraries) a VOLSER range has to be defined in the library GUI.
It separates the cartridges between logical libraries.

!!! tip
    The assignment of VOLSER ranges may optionally be used by administrators to convey information by convention, such as a dedicated range for cartridges used for testing only, or to indicate media generation at a glance.
    For instance, tapes in the range I9XXXX could be assigned to LTO9 tapes inside of an IBM library, making these easy for operators to identify., making these easy for operators to identify.


## Tape Format

Data written to tape may be structured using a number of different formats.
Some of these are *self-describing*, such that the metadata generally associated with files, like file names, are stored on the media itself, together with the file data.
An example of such a format is LTFS.

CTA has its own data format, which is also called `CTA`.
This format stems from CTA's predecessor at CERN, known as CASTOR, and so it is identical in all but name.
As a consequence of this, CTA is capable of reading media which was written to by CASTOR, allowing for easy migrations without having to re-write the media.

Note that the CTA/CASTOR formats are *not* self-describing, meaning that one has to take good care of the metadata stored within the CTA catalogue.

The [CTA Tape Format](./format.md) page gives a detailed description of what the CTA format looks like on tape.

### Labeling a tape

!!! danger
    Labeling a tape is a destructive action which *overwrites* any data on said tape. Never label a tape with data on it. There is no way to recover the data, apart from having the vendor attempt a recovery.

Before a tape can be written to by CTA, it must have the CTA format applied by *labeling* the tape.
The labeling procedure applies the CTA tape format, and specifically the VOL1 descriptor containing the tape's VID, to the beginning of the tape.
This VID field is used to verify that the tape's content is what one expected, based on the VID printed on the physical cartridge.
Individual tapes may be labeled using the `cta-tape-label` command line tool.
However, we strongly recommend using the wrapper command [cta-ops-admin tape label](./../../../ops/tools/ops-utils/cta-ops-admin.md), which supports bulk-labeling a number of tapes sequentially, and performs safety checks before doing destructive actions.

### Read-only formats

!!! note
    When writing new data only the CTA format is supported.

CTA also supports a set of additional tape formats for read-only operations.
These allow CTA adopters to use their existing tapes, without having to re-write data to the CTA/CASTOR format.

#### OSM

The OSM tape label format is supported in CTA from version `4.7.9-1` and later.

#### Enstore

The Enstore tape label format, based on CPIO, is supported in CTA from version `4.7.11-1` and later.

Additionally, support for the Enstore Large format, which includes support for files exceeding 8GB in size, was added in CTA versions `v4.10.11.0-1` and `v5.10.11.0-1`.

## Tape Media and CTA

In CTA the state and metadata of each tape cartridge is individually tracked.
Tapes are organized into Tape Pools, indicating the ownership of the tape and the data on it.
The tape pool system is structured that a tape can belong to at most one VO at a time.
Shared ownership of one tape between multiple VOs is not supported.

### Media properties

Besides the VID, CTA keeps track of a number of properties associated to each tape, which may be viewed using the `cta-admin tape ls` command.

!!! tip
    Use the `--json` flag to view additional fields

Some of these are for record keeping purposes, while others impact the behavior of CTA.
Some notable of the latter are:

* **mediaType:** The cartridge format and generation, such as `LTO9`
* **logicalLibrary:** The assigned Logical Library
* **tapepool:** The Tape Pool the cartridge belongs to
* **vo:** The VO that owns the data on this media
* **encryptionKeyName:** The identified for the key used to encrypt this media, if applicable
* **full:** Whether or not the tape is considered to be full, i.e. whether it can no longer be written to
* **nbMasterFiles:** The number of non-deleted files on this tape
* **nbMasterBytes:** Data volume corresponding to the nbMasterFiles count
* **state:** The present operation state of the tape, see below

!!! note
    Some CTA operations don't trigger immediate counter and metadata updates. This includes fields such as  `nbMasterfiles` and `nbMasterBytes`. Use the [cta-statistics-update](./../../../ops/tools/cta-statistics-update.md) tool to refresh these.



### Life Cycle

A tape cartridge starts its stay in CTA by being added to the catalogue, and then being labeled by an operator.
Depending on the media generation, if the cartridge has not been pre-initialized, this initial labeling procedure is accompanied by an initialization process which calibrates the media to the library's local environmetal conditions.
Be aware that this process may take as much as 50 minute per cartridge.

Once labeled and initialized, the tape can be assigned to an appropriate Tape Pool for use.

In CTA, each tape cartridge has a *state*, which determines what actions may be performed on it.
A tape may for instance be 'ACTIVE', indicating that it can be read from and written to, or `DISABLED`, such that neither reads nor writes may be performed.
A detailed description of each media state is given in [the Tape Lifecycle page](../lifecycle.md)

#### Repack

On occasion, tape media may become damaged, putting the availability and integrity of its data at risk.
When this happens, it is imperative to migrate the data to a new, healthy tape.

Depending on the data stored, it may also be a good idea to keep multiple copies of certain data, as a precaution for any such failure condition.
To achieve this, data must be copied from one tape onto another.

Additionally, as tape media technology evolves, the per-cartridge density tends to increase significantly, which in turn increases the potential storage provided by each licensed cartridge slot in the library.
Combined with CTA's most common use case of indefinite data storage for physics, and the need for the infrastructure to stay on supported hardware, this creates an incentive to periodically move data from old media generations to new ones.

The combination of these three make up the Repack use case, that is, the copying/moving of data from one tape to another.
CTA has a dedicated [repack workflow](./../../../dev/architecture/workflows/repack.md), one can also find examples of how to use the `cta-admin` command for managing individual repacks.
For larger repack batches, [a dedicated operator utility](./../../../ops/tools/ops-utils/atresys.md) is provided to manage the repacks at a higher level.

Once a tape is repacked, it should be empty and left completely without files.
In the case of media generation changes, the repacked tape is now ready to be removed from the library and from CTA.
If the repack was initiated due to issues with the media, one may [perform a media check](./../../../ops/tools/ops-utils/cta-ops-admin.md#tape-tools) to see whether or not the media was truly damaged, or if the cartridge may be re-used another time.
