# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""udev-backed implementations of the repository protocols.

See [cta.tapeserver.sdk.repositories.protocols][cta.tapeserver.sdk.repositories.protocols]
for the public contracts. The classes in this package enumerate
locally-attached SCSI tape hardware via pyudev and resolve the
parent/child links between drives and their parent medium changer.

Two families of repositories are exposed, one per protocol layer:

* Drives — [LocalDriveRepository][cta.tapeserver.sdk.repositories.local.drive.LocalDriveRepository],
  [LocalNamedDriveRepository][cta.tapeserver.sdk.repositories.local.drive.LocalNamedDriveRepository],
  [LocalLocatedDriveRepository][cta.tapeserver.sdk.repositories.local.drive.LocalLocatedDriveRepository],
  [LocalLocatedNamedDriveRepository][cta.tapeserver.sdk.repositories.local.drive.LocalLocatedNamedDriveRepository].
* Libraries — [LocalLibraryRepository][cta.tapeserver.sdk.repositories.local.library.LocalLibraryRepository]
  and [LocalNamedLibraryRepository][cta.tapeserver.sdk.repositories.local.library.LocalNamedLibraryRepository].
"""
