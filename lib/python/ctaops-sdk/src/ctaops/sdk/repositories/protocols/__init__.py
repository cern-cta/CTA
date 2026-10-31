# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Repository protocols and metadata dataclasses.

These protocols describe the read-only infrastructure topology — visible
tape drives and libraries — that the rest of the SDK builds on top of.
The dataclasses they expose
([Drive][ctaops.sdk.repositories.protocols.drive.Drive],
[NamedDrive][ctaops.sdk.repositories.protocols.drive.NamedDrive],
[LocatedDrive][ctaops.sdk.repositories.protocols.drive.LocatedDrive],
[LocatedNamedDrive][ctaops.sdk.repositories.protocols.drive.LocatedNamedDrive],
[Library][ctaops.sdk.repositories.protocols.library.Library],
[NamedLibrary][ctaops.sdk.repositories.protocols.library.NamedLibrary])
are immutable value objects.

Concrete udev-backed implementations:
[LocalLibraryRepository][ctaops.sdk.repositories.local.library.LocalLibraryRepository]
and
[LocalDriveRepository][ctaops.sdk.repositories.local.drive.LocalDriveRepository]
(plus their `Named*` / `Located*` variants).
"""
