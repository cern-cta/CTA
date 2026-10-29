# Usage

This section presents code examples to use the `ctaops-sdk`.

## Discover libraries and drives

The SDK exposes one repository per (named?, located?) combination. Pick the variant that matches the metadata your callers need.

```python
from ctaops.sdk import (
    LocalLibraryRepository, LocalNamedLibraryRepository,
    LocalDriveRepository, LocalNamedDriveRepository,
    LocalLocatedDriveRepository, LocalLocatedNamedDriveRepository,
)

# Libraries
libs = LocalLibraryRepository().list_libraries()                  # set[Library]
named_libs = LocalNamedLibraryRepository().list_libraries()       # set[NamedLibrary]
lib = LocalNamedLibraryRepository().get_library_by_name("T0-LIB0")  # NamedLibrary

# Drives — base view
drives = LocalDriveRepository().list_drives()                     # set[Drive]
named_drives = LocalNamedDriveRepository().list_drives()          # set[NamedDrive]

# Drives bound to a specific library
drv_repo = LocalLocatedNamedDriveRepository()
drives_in_lib0 = drv_repo.list_drives(lib_serial_number="LIBSN")  # set[LocatedNamedDrive]

# Direct lookup
drive = drv_repo.get_drive_by_name("T0-LIB0-DRV0")                # LocatedNamedDrive
drive = drv_repo.get_drive_by_ordinal(0, "LIBSN")                 # LocatedNamedDrive
```

## Resolve a library controller

The recommended entry point is [`ScsiManagedLibraryControllerProvider`](../../../../api/python/ctaops-sdk/reference/controller-providers.md#ctaops.sdk.controller_providers.scsi.library.ScsiManagedLibraryControllerProvider), which yields a fully-wired [`ScsiManagedLibraryController`](../../../../api/python/ctaops-sdk/reference/controllers.md#ctaops.sdk.controllers.scsi.library.ScsiManagedLibraryController):

```python
from ctaops.sdk import ScsiManagedLibraryControllerProvider

provider = ScsiManagedLibraryControllerProvider()
lib = provider.get_library_controller_by_name("T0-LIB0")
# or by serial:
# lib = provider.get_library_controller("LIB-SERIAL-1234")

print(f"Library {lib.name} (serial {lib.serial_number})")
print(f"I/O slots: {lib.get_max_io_slots()}")
```

If you only need ordinal-only addressing (no logical-name resolution, no `label_tape`), use the lighter [`ScsiLibraryControllerProvider`](../../../../api/python/ctaops-sdk/reference/controller-providers.md#ctaops.sdk.controller_providers.scsi.library.ScsiLibraryControllerProvider) which yields a [`ScsiLibraryController`](../../../../api/python/ctaops-sdk/reference/controllers.md#ctaops.sdk.controllers.scsi.library.ScsiLibraryController).

You can also resolve the managed library that hosts a given drive — handy when callers only know the drive's logical name and not the parent library:

```python
provider = ScsiManagedLibraryControllerProvider()
lib = provider.get_library_controller_from_drive_name("T0-LIB0-DRV0")
# or the non-raising variant:
maybe_lib = provider.find_library_controller_from_drive_name("T0-LIB0-DRV0")
```

## Mount a tape and perform I/O

`ManagedLibraryController.mount` returns a `MountedDriveController` that is
also a context manager — leaving the `with` block automatically unmounts the tape, even on error.

```python
from ctaops.sdk import ScsiManagedLibraryControllerProvider, EOTError, EODError

lib = ScsiManagedLibraryControllerProvider().get_library_controller_by_name("T0-LIB0")

# Drive can be referenced by ordinal (int) or by ctaops name (str):
with lib.mount(drive_key="T0-LIB0-DRV0", vid="ABC123") as mounted:
    print(f"Mounted {mounted.mounted_vid}, VOL1={mounted.get_VOL1()!r}")

    file_size  = 100 * 1024 * 1024   # 100 MiB per file
    block_size = 256 * 1024          # 256 KiB

    try:
        for file_idx, checksum in mounted.write_files(
            file_size=file_size, n=100, block_size=block_size
        ):
            print(f"wrote file {file_idx} adler32={checksum:08x}")
    except EOTError as e:
        print(f"tape full after {e.written_files} files (last={e.write_checksum:08x})")

    mounted.rewind()  # skips VOL1 by default

    try:
        for file_idx, checksum in mounted.read_files(n=100, block_size=block_size):
            print(f"read file {file_idx} adler32={checksum:08x}")
    except EODError:
        print("end of recorded data")
```

## Inspect tape location and drive state

```python
from ctaops.sdk import ScsiManagedLibraryControllerProvider

lib = ScsiManagedLibraryControllerProvider().get_library_controller_by_name("T0-LIB0")

loc = lib.get_tape_location("ABC123")       # TapeLocation
print(loc.location, loc.address, loc.library_serial_number)

if lib.drive_is_free("T0-LIB0-DRV0"):
    print("drive is free for mount")
```

## Drive-only operations (no library)

For unmanaged drives — or for cases where you want a drive controller without a library context — use [`ScsiDriveControllerProvider`](../../../../api/python/ctaops-sdk/reference/controller-providers.md#ctaops.sdk.controller_providers.scsi.drive.ScsiDriveControllerProvider):

```python
from ctaops.sdk import ScsiDriveControllerProvider

drives = ScsiDriveControllerProvider()
drive = drives.get_drive_controller_by_name("T0-LIB0-DRV0")

print(f"serial   : {drive.serial_number}")
print(f"loaded?  : {drive.has_loaded_tape()}")
print(f"temp °C  : {drive.get_drive_temperature()}")
print(f"humidity : {drive.get_drive_humidity()}")

drive.set_compression(1)
drive.unload()
```

## Tape labeling

```python
from ctaops.sdk import ScsiManagedLibraryControllerProvider

lib = ScsiManagedLibraryControllerProvider().get_library_controller_by_name("T0-LIB0")

# New tape:
lib.label_tape(drive_key="T0-LIB0-DRV0", vid="NEW001")

# Relabel an existing one (requires force=True and may require old_vid):
lib.label_tape(drive_key=0, vid="NEW002", old_vid="OLD002", force=True)
```

## Library inventory operations

```python
lib.import_tapes()              # move tapes from import/export mailslot to slots
lib.export_tape("ABC123")       # move a tape to the import/export mailslot
lib.unmount("T0-LIB0-DRV0")     # idempotent — no-op if drive already free
```

## Error handling

Every library-/drive-/tape-related error inherits from `CtaopsSdkError`:

```
CtaopsSdkError
├── ResourceNotFoundError
├── LibraryOperationError
├── DriveError
│   ├── DriveStateError
│   ├── DriveOperationError
│   └── DriveIOError
│       ├── EOTError       # carries `write_checksum` and `written_files`
│       └── EODError
└── TapeError
    ├── TapeStateError
    └── TapeLabelError
```

```python
from ctaops.sdk import (
    ScsiManagedLibraryControllerProvider,
    EOTError, EODError, TapeLabelError,
    ResourceNotFoundError, DriveStateError,
)

try:
    lib = ScsiManagedLibraryControllerProvider().get_library_controller_by_name("T0-LIB0")
    with lib.mount("T0-LIB0-DRV0", "ABC123") as mounted:
        for idx, checksum in mounted.write_files(file_size=100 * 1024 * 1024, n=1000):
            ...
except EOTError as e:
    print(f"end-of-tape: {e.written_files} files, last checksum {e.write_checksum:08x}")
except DriveStateError as e:
    print(f"drive not in expected state: {e}")
except ResourceNotFoundError as e:
    print(f"library or drive not found: {e}")
```
