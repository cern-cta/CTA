# Overview

**ctaops-sdk** is a Python library for managing CTA (CERN Tape Archive) tape
server hardware locally. It provides a Pythonic, type-annotated interface to discover, configure and operate tape libraries (medium changers) and tape drives on a CTA tape server system.

**Key features:**

- 🔍 **udev-driven discovery** — locate libraries and drives via udev metadata, no static config files.
- 🏗️ **Three-layer architecture** — clean separation between *what is here* (repositories), *what to do with it* (controllers), and *how to obtain a controller* (controller providers).
- 🧩 **Protocol-based design** — repository, controller, and controller-provider layers are defined as `typing.Protocol`s, so alternative backends (mock, gRPC, …) can be plugged in without touching call sites.
- 🔒 **Fully typed** — Python 3.9+ type hints, `py.typed` marker, dedicated value objects for each level of binding (`Drive`/`NamedDrive`/`LocatedDrive`/`LocatedNamedDrive`).
- 🔁 **Context-managed mounts** — `library.mount(...)` returns a context manager that always returns the tape to its slot, even on errors or end-of-tape.
- 🌡️ **Hardware telemetry** — read drive temperature, humidity, write-protect, and online status via `mt` and `sg_logs`.
- ✍️ **Streaming I/O** — generator-based `read_files` / `write_files` with Adler-32 checksumming and EOT/EOD signalling.
- 📝 **Stdlib logging** — every subprocess invocation and state transition is logged on the `ctaops.sdk` logger.

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

Please see [API Usage](api-usage.md) page.

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
