# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""udev-backed implementations of the repository protocols.

See [ctaops.sdk.repositories.protocols][ctaops.sdk.repositories.protocols]
for the public contracts. The classes in this package enumerate
locally-attached SCSI tape hardware via pyudev and resolve the
parent/child links between drives and their parent medium changer.

Two families of repositories are exposed, one per protocol layer:

* Drives — [LocalDriveRepository][ctaops.sdk.repositories.local.drive.LocalDriveRepository],
  [LocalNamedDriveRepository][ctaops.sdk.repositories.local.drive.LocalNamedDriveRepository],
  [LocalLocatedDriveRepository][ctaops.sdk.repositories.local.drive.LocalLocatedDriveRepository],
  [LocalLocatedNamedDriveRepository][ctaops.sdk.repositories.local.drive.LocalLocatedNamedDriveRepository].
* Libraries — [LocalLibraryRepository][ctaops.sdk.repositories.local.library.LocalLibraryRepository]
  and [LocalNamedLibraryRepository][ctaops.sdk.repositories.local.library.LocalNamedLibraryRepository].
"""
