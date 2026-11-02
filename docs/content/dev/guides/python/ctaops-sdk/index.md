# Overview

`ctaops-sdk` discovers and operates tape libraries and drives on a CTA tape server through udev metadata and SCSI utilities.
It provides typed interfaces for mounting tapes, reading and writing data, and inspecting hardware state.

- Mount context managers handle unmounting when the context exits.
- `read_files` and `write_files` use generators, Adler-32 checksums, and end-of-tape or end-of-data signals.
- Hardware queries report temperature, humidity, write protection, and online status.
- The `ctaops.sdk` logger records subprocess calls and state changes.

The package is organised in three layers, each with a `Protocol`-based interface and a concrete SCSI/udev-backed implementation:

| Layer | Purpose | Protocols | Concrete implementations |
| --- | --- | --- | --- |
| Repositories | Discover libraries and drives via udev metadata | `LibraryRepository`, `NamedLibraryRepository`, `DriveRepository`, `NamedDriveRepository`, `LocatedDriveRepository`, `LocatedNamedDriveRepository` | `LocalLibraryRepository`, `LocalNamedLibraryRepository`, `LocalDriveRepository`, `LocalNamedDriveRepository`, `LocalLocatedDriveRepository`, `LocalLocatedNamedDriveRepository` |
| Controllers | Operate a single library / drive (mount, unmount, label, I/O, …) | `LibraryController`, `ManagedLibraryController`, `DriveController`, `MountedDriveController` | `ScsiLibraryController`, `ScsiManagedLibraryController`, `ScsiDriveController`, `ScsiMountedDriveController` |
| Controller providers | Resolve a controller by serial number or ctaops name | `DriveControllerProvider`, `LibraryControllerProvider`, `ManagedLibraryControllerProvider` | `ScsiDriveControllerProvider`, `ScsiLibraryControllerProvider`, `ScsiManagedLibraryControllerProvider` |

## Installation
!!! abstract "Command"
    ```bash
    python3 -m pip install ctaops-sdk \
        --extra-index-url https://cta-public-repo.web.cern.ch/cta-operations/stable/pypi/simple/
    ```

### System requirements

The library shells out to standard SCSI/tape utilities, which must be present on the tape server:

- `/usr/bin/cta-smc` – SCSI Media Changer control utility
- `/usr/bin/cta-tape-label` – CTA tape labeling tool
- `/usr/bin/sg_logs` – SCSI log page viewer (used for temperature / humidity)
- `/bin/mt` – magnetic tape control

### udev metadata requirements

Discovery is driven entirely by udev properties. Every CTA-managed device must carry these properties (typically populated by the [`ctaops-udev`](../../../../ops/deploy-and-configure/deployment/ctaops-udev.md) rules package — provided as-is, see its docs for the caveats):

| Property | Library (sg) | Drive (nst) |
| --- | :---: | :---: |
| `ID_SCSI_SERIAL` | required | required |
| `CTAOPS_NAME` | required for `Named*` variants | required for `Named*` variants |
| `SCSI_TYPE=medium changer` | required | – |
| `CTAOPS_LIBRARY_ADDRESS` | – | required for `Located*` variants |
| `CTAOPS_LIBRARY_ORDINAL` | – | required for `Located*` variants |

Devices missing the metadata required for the variant you queried are silently skipped during discovery.

## Usage

See [Usage](api-usage.md) for examples.

## Type hints

The package is fully type-annotated and ships a `py.typed` marker. All protocols, providers, and metadata dataclasses are re-exported from the top-level `ctaops.sdk` namespace so static analysis works without reaching into private modules:

```python
from ctaops.sdk import (
    # Controller protocols
    DriveController, MountedDriveController,
    LibraryController, ManagedLibraryController,
    TapeLocation,
    # Controller-provider protocols
    DriveControllerProvider,
    LibraryControllerProvider, ManagedLibraryControllerProvider,
    # Repository value objects
    Drive, NamedDrive, LocatedDrive, LocatedNamedDrive,
    Library, NamedLibrary,
    DriveLibraryLocation,
    # Repository protocols
    DriveRepository, NamedDriveRepository,
    LocatedDriveRepository, LocatedNamedDriveRepository,
    LibraryRepository, NamedLibraryRepository,
)
```

Drive value objects progressively add metadata:

- `Drive` — bare-bones, just a SCSI serial.
- `NamedDrive` — adds `CTAOPS_NAME`.
- `LocatedDrive` — adds the parent library binding (`DriveLibraryLocation`).
- `LocatedNamedDrive` — adds both name and location.

The same staircase applies to libraries: `Library` → `NamedLibrary`.



## License

GPL-3.0-or-later Copyright © 2026 CERN
