# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""ctaops-sdk: Python library for managing CTA tape server hardware locally.

The package layers cleanly into three pieces, each with a small set of
[typing.Protocol][typing.Protocol] interfaces and SCSI/udev-backed
implementations:

* **Repositories** — read-only enumeration of visible tape drives and
  libraries. The
  [LibraryRepository][ctaops.sdk.repositories.protocols.library.LibraryRepository]
  /
  [NamedLibraryRepository][ctaops.sdk.repositories.protocols.library.NamedLibraryRepository]
  and `*DriveRepository` protocols (see
  [DriveRepository][ctaops.sdk.repositories.protocols.drive.DriveRepository]
  and friends) describe the contract; concrete implementations live
  under `ctaops.sdk.repositories.local` (e.g.
  [LocalLibraryRepository][ctaops.sdk.repositories.local.library.LocalLibraryRepository]).
* **Controllers** — operate the hardware. They implement the
  [DriveController][ctaops.sdk.controllers.protocols.drive.DriveController] /
  [MountedDriveController][ctaops.sdk.controllers.protocols.drive.MountedDriveController] /
  [LibraryController][ctaops.sdk.controllers.protocols.library.LibraryController] /
  [ManagedLibraryController][ctaops.sdk.controllers.protocols.library.ManagedLibraryController]
  protocols. Use the protocol types for type-hints; the concrete
  SCSI-backed classes are
  [ScsiDriveController][ctaops.sdk.controllers.scsi.drive.ScsiDriveController],
  [ScsiMountedDriveController][ctaops.sdk.controllers.scsi.drive.ScsiMountedDriveController],
  [ScsiLibraryController][ctaops.sdk.controllers.scsi.library.ScsiLibraryController],
  and
  [ScsiManagedLibraryController][ctaops.sdk.controllers.scsi.library.ScsiManagedLibraryController].
* **Controller providers** — preferred entry points that wire the above
  together with a single shared pyudev context:
  [ScsiManagedLibraryControllerProvider][ctaops.sdk.controller_providers.scsi.library.ScsiManagedLibraryControllerProvider]
  and
  [ScsiDriveControllerProvider][ctaops.sdk.controller_providers.scsi.drive.ScsiDriveControllerProvider].

Logging is emitted under the `ctaops.sdk` logger; configure it like any
other [logging][] logger to see subprocess invocations and state
transitions.

Example:
    ```python
    import logging
    from ctaops.sdk import ScsiManagedLibraryControllerProvider

    logging.getLogger("ctaops.sdk").setLevel(logging.INFO)

    library = ScsiManagedLibraryControllerProvider().get_library_controller_by_name("lib-0")
    with library.mount("drive-0", "X12345") as drive:
        drive.set_block_size(256 * 1024)
        for index, checksum in drive.write_files(file_size=1 << 30, n=5):
            print(index, checksum)
    ```
"""

from ctaops.sdk.controller_providers.protocols.drive import DriveControllerProvider
from ctaops.sdk.controller_providers.protocols.library import (
    LibraryControllerProvider,
    ManagedLibraryControllerProvider,
)
from ctaops.sdk.controller_providers.scsi.drive import ScsiDriveControllerProvider
from ctaops.sdk.controller_providers.scsi.library import (
    ScsiLibraryControllerProvider,
    ScsiManagedLibraryControllerProvider,
)
from ctaops.sdk.controllers.protocols.drive import (
    DriveController,
    MountedDriveController,
)
from ctaops.sdk.controllers.protocols.library import (
    LibraryController,
    ManagedLibraryController,
    TapeLocation,
)
from ctaops.sdk.exceptions import (
    CtaopsSdkError,
    DriveError,
    DriveIOError,
    DriveOperationError,
    DriveStateError,
    EODError,
    EOTError,
    LibraryOperationError,
    ResourceNotFoundError,
    TapeError,
    TapeLabelError,
    TapeStateError,
)
from ctaops.sdk.repositories.local.drive import (
    LocalDriveRepository,
    LocalLocatedDriveRepository,
    LocalLocatedNamedDriveRepository,
    LocalNamedDriveRepository,
)
from ctaops.sdk.repositories.local.library import (
    LocalLibraryRepository,
    LocalNamedLibraryRepository,
)
from ctaops.sdk.repositories.protocols.drive import (
    Drive,
    DriveLibraryLocation,
    DriveRepository,
    LocatedDrive,
    LocatedDriveRepository,
    LocatedNamedDrive,
    LocatedNamedDriveRepository,
    NamedDrive,
    NamedDriveRepository,
)
from ctaops.sdk.repositories.protocols.library import (
    Library,
    LibraryRepository,
    NamedLibrary,
    NamedLibraryRepository,
)

__all__ = [
    "CtaopsSdkError",
    "ResourceNotFoundError",
    "DriveError",
    "TapeError",
    "LibraryOperationError",
    "DriveIOError",
    "DriveOperationError",
    "DriveStateError",
    "EODError",
    "EOTError",
    "TapeLabelError",
    "TapeStateError",
    "DriveControllerProvider",
    "LibraryControllerProvider",
    "ManagedLibraryControllerProvider",
    "ScsiDriveControllerProvider",
    "ScsiLibraryControllerProvider",
    "ScsiManagedLibraryControllerProvider",
    "DriveController",
    "MountedDriveController",
    "LibraryController",
    "ManagedLibraryController",
    "TapeLocation",
    "DriveLibraryLocation",
    "Drive",
    "NamedDrive",
    "LocatedDrive",
    "LocatedNamedDrive",
    "DriveRepository",
    "NamedDriveRepository",
    "LocatedDriveRepository",
    "LocatedNamedDriveRepository",
    "Library",
    "NamedLibrary",
    "LibraryRepository",
    "NamedLibraryRepository",
    "LocalDriveRepository",
    "LocalNamedDriveRepository",
    "LocalLocatedDriveRepository",
    "LocalLocatedNamedDriveRepository",
    "LocalLibraryRepository",
    "LocalNamedLibraryRepository",
]
