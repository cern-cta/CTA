# Tape Drives

Tape drives are the devices responsible for writing data to tape media, and for reading them back.
Just like with the tape media, the drives belong to either the LTO or IBM3592 family, and each drive has a range of supported tape cartridge models.

These tape drives are normally placed inside of a tape library, and connected to a *tape daemon*, which controls it.
At CERN, the tape drives are connected using SCSI, so in these pages we will work with that assumption.

Furthermore, drives may come in full- and half-height editions.
Full-height drives have more features, enabling better performance, and are generally found inside of tape libraries.
Half-height drives have a more restricted set of features, but may be used from an operator's desk.
We will assume use of full-height drives in these pages.

## Drives and libraries

A tape library will identify a drive by its *ordinal*, which is the a number between 0 and 257.
The ordinal number is the same as the `elementAddress` - 257 on IBM libraries, and the same as the `partitionDriveNumber` - 1 on a Spectra Logic library.

Using the `cta-smc` command line tool, a drive's ordinal can be found:

```bash
    # [root@tpsrvXXX ~]# cta-smc -q D
    # Drive Ordinal   Element Addr.     Status     Vid
    #             0             257       free
    #             1             258       free
    #             2             259       free
    #             3             260       free
```

## Tape Drives and CTA

### Naming Conventions


