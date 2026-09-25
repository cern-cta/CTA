# Tape Libraries

Tape libraries are home to the tape media and tape drives, as well as everything needed in terms of robotics and communications to move the media between their home slot and a waiting drive.
It is the job of the library to keep track of which tape is where, and to respond to requests for the mounting and un-mounting of tapes.

## Physical libraries

Physical libraries vary from manufacturer to manufacturer in terms of terminology and functionality.
We recommend studying the manuals provided by your vendor.

The examples here draw on IBM TS4500 and Spectra Logic TFinity libraries used at CERN.
Some terms to know include:

- **Storage slots** hold cartridges when they are not mounted.
- **Import/export slots** allow cartridges to be inserted into or removed from the library.
- **Drives** read and write mounted cartridges.
- **Robotics** move cartridges between slots and drives, under the control of the library.

### Partitions

A tape library can be partitioned into separate sets of tape media and drives, called hardware partitions.
The partitioning mechanism is vendor-specific.
Partitions act as hardware groupings which are independent from one another, and allow for features such as:

* Running CTA alongside other magnetic tape data management software products
* On libraries that support it, isolating cartridges in partitions without drives to prevent normal drive access (for example, vendor-specific *Safeguarded Tape* features)

## Library control and inventory

CTA selects tape work through the scheduler. The tape daemon requests a mount through the [Media Changer Daemon](../components/media-changer-daemon.md) (`cta-rmcd`), which asks the library robotics to move the cartridge into the selected drive. File data passes through the tape drive, not through the robotics interface.

The library tracks the physical locations of cartridges. CTA's catalogue tracks tape identities, library membership, and file copies. Registering a tape or changing its catalogue assignment does not physically move it or change a hardware partition.

## Libraries in CTA

### Physical libraries

A physical-library record is optional in CTA. It groups logical libraries for administration and records information about the hardware, such as slot counts. These catalogue values should not be confused with an automatically discovered, live hardware inventory.

Associating logical libraries with a physical-library record also allows the corresponding resources to be disabled together for maintenance. See [Tapes, Drives, and Libraries](../../ops/administration/tapes-and-drives.md) for registration and state changes.

### Logical libraries

A *logical library* in CTA groups tapes and drives that can be used together. It may be associated with a physical-library record, but does not require one.

CTA logical libraries are administrative groupings, not representations that must match hardware partitions one-to-one. Operators can define their own groupings within the same hardware partition—for example, to separate media generations or dedicate sets of drives and tapes to different workloads. Creating these groups does not partition the hardware or enforce isolation in the library itself.

The grouping must still make physical sense: drives assigned to a CTA logical library must be able to mount and use the tapes assigned to it. A catalogue assignment cannot make a cartridge accessible across an inaccessible hardware partition, or make incompatible media readable by a drive. Conversely, sharing a hardware partition does not make drives in another CTA logical library eligible to serve those tapes; CTA uses logical-library membership when selecting work.
