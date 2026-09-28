---
title: Tape media overview
---

# Tape Media

*Tape media* refers to the magnetic tape data storage device itself, which in the present generation of tape technology takes the shape of a *cartridge* filled with a magnetic tape band on which the data is written.

## Cartridge

A cartridge is stored inside of a tape library, and read/written by a tape drive.
Generally, it contains about 1km worth of 12.65mm wide magnetic tape band.

Data is written on this tape in a *boustrophedon* manner:
Bits are written on to a set of parallel *tracks*, which are organised in *wraps* spanning from one end of the tape to the other.
Where one wrap ends, another begins, going in the opposite direction.

## Cartridge Formats

At present, two media types are actively developed and produced:

1. LTO
2. IBM3592 (also referred to as Enterprise)

Both of these are supported by CTA.

### Identifiers: VID/VOLSER

CTA identifies a tape cartridge by its six-character (`[A-Z0-9]{6}`) **volume identifier (VID)**, also called its **volume serial number (VOLSER)**. This is distinct from the full physical barcode: an LTO barcode includes a media identifier after the six-character volume ID. See [Oracle’s cartridge-label guidance](https://docs.oracle.com/cd/E35103_06/en/E24606/html/loading-cartridges.htm).
The VID of each cartridge must be unique across the CTA catalogue, including cartridges in different libraries.

Usually, a range of VIDs is specified by the administrator at the time of media purchase and printed on stickers that are put on the cartridges by the supplier.
There is no particular rule as to which range is assigned to which kind of cartridges.
Partitioning rules depend on the library model and configuration. Some libraries use VOLSER ranges to assign cartridges to partitions; follow the vendor’s guidance and keep the resulting placement consistent with CTA’s logical-library assignments.

!!! tip
    The assignment of VOLSER ranges may optionally be used by administrators to convey information by convention, such as a dedicated range for cartridges used for testing only, or to indicate media generation at a glance.
    For instance, tapes in the range I9XXXX could be assigned to LTO9 tapes inside of an IBM library, making these easy for operators to identify.

## Tape Format

Data written to tape may be structured using a number of different formats.
Some of these are *self-describing*, such that the metadata generally associated with files, like file names, are stored on the media itself, together with the file data.
An example of such a format is LTFS.

CTA writes data in the CTA format, inherited from its predecessor CASTOR. Compatibility with CASTOR's AUL format allows existing tapes to be read without rewriting their data.

Note that the CTA format is *not* self-describing, meaning that one has to take good care of the metadata stored within the CTA catalogue.

The [CTA Tape Format](format.md) page gives a detailed description of what the CTA format looks like on tape.

### Labelling a tape

Labelling writes the tape format and volume identifier so CTA can verify the medium it has mounted. This is a destructive procedure and should not be done on tapes with active data. The operator procedure for this is documented under [Media Initialisation](../../../ops/run-and-maintain/administration/media-initialisation.md).

### Read-only formats

When writing new data only the CTA format is supported, but CTA also supports a set of additional tape formats for read-only operations.
These allow CTA adopters to use their existing tapes, without having to re-write data to the CTA format. The following formats are supported:

- OSM
- Enstore
- Enstore Large

!!! note "Additional tape formats"
    CTA can be extended to read other tape formats when their layout is known. To discuss support for a format you use, ask on the [CTA community forum](https://cta-community.web.cern.ch/), or contribute a reader implementation yourself.

## Tape Media and CTA

CTA tracks the state and metadata of each tape cartridge individually. Each tape belongs to one **tape pool**, a group of tapes used for data placement. Each pool belongs to a **virtual organisation (VO)**, which represents an administrative owner such as an experiment or project. A tape therefore belongs to one VO through its pool; shared ownership between VOs is not supported.

These ownership and placement relationships are explained in [Storage Model](../../data-management/storage-model.md#disk-instances-and-virtual-organisations).

### Media properties

Besides the VID, CTA keeps track of a number of properties associated to each tape, which may be viewed using the `cta-admin tape ls` command.

!!! tip
    Use the `--json` flag to view additional fields

Some of these are for record keeping purposes, while others impact the behaviour of CTA.
Some notable of the latter are:

* **mediaType:** The cartridge format and generation, such as `LTO9`
* **logicalLibrary:** The assigned logical library
* **tapepool:** The tape pool the cartridge belongs to
* **vo:** The virtual organisation associated with the tape pool
* **encryptionKeyName:** The identifier for the key used to encrypt this media, if applicable
* **full:** Whether or not the tape is considered to be full, i.e. whether it can no longer be written to
* **nbMasterFiles:** The number of non-deleted files on this tape
* **nbMasterBytes:** Data volume corresponding to the nbMasterFiles count
* **state:** The present operational state of the tape, see below

!!! note
    Some CTA operations don't trigger immediate counter and metadata updates. This includes fields such as  `nbMasterFiles` and `nbMasterBytes`. Use the [cta-statistics-update](../../../ops/tools/cta-statistics-update.md) tool to refresh these.

### Lifecycle

A tape cartridge is first registered in the catalogue with an existing tape pool and logical library, then labelled by an operator.
Some media require a separate drive-level optimization on first load. This is distinct from writing CTA labels. For LTO-9 L9/LZ media, optimization calibrates the cartridge for data placement and can take up to two hours; timing depends on the media and drive. See [IBM’s media optimization guidance](https://www.ibm.com/docs/en/ts4300-tape-library?topic=features-media-optimization).

Once labelled and initialised, the tape can be used when its state and other scheduling conditions allow it. Its tape-pool assignment can be changed separately if required. See [Media Initialisation](../../../ops/run-and-maintain/administration/media-initialisation.md) for the operator workflow.

In CTA, each tape cartridge has a *state*, which determines what actions may be performed on it.
An `ACTIVE` tape is eligible for reads and, when writable and not full, writes. A `DISABLED` tape cannot be mounted, although normal retrieval requests can still queue for it.
A detailed description of each media state is given in [the Tape Lifecycle page](../lifecycle.md).

#### Repack

On occasion, tape media may become damaged, putting the availability and integrity of its data at risk.
When this happens, it is imperative to migrate the data to a new, healthy tape.

Depending on the data stored, it may also be a good idea to keep multiple copies of certain data, as a precaution for any such failure condition.
To achieve this, data must be copied from one tape onto another.

Additionally, as tape media technology evolves, the per-cartridge density tends to increase significantly, which in turn increases the potential storage provided by each licensed cartridge slot in the library.
Combined with CTA's most common use case of indefinite data storage for physics, and the need for the infrastructure to stay on supported hardware, this creates an incentive to periodically move data from old media generations to new ones.

The combination of these three make up the Repack use case, that is, the copying/moving of data from one tape to another.
CTA has a dedicated [repack workflow](../../data-management/repack.md). Operator commands are documented under [Repacking Tapes](../../../ops/run-and-maintain/administration/repack.md).
For larger repack batches, [a dedicated operator utility](../../../ops/tools/repack-automation.md) is provided to manage the repacks at a higher level.

After a successful repack that moves all active tape copies off the source tape, it no longer holds active file copies in the catalogue. Repacking does not physically erase the data on the source tape; reclaiming it for reuse is a separate operation.
In the case of media generation changes, the source tape can then be retired according to the site's procedures.
If the repack was initiated due to issues with the media, one may [perform a media check](../../../ops/tools/cta-ops-admin.md#tape-tools) to see whether or not the media was truly damaged, or if the cartridge may be re-used another time.
