# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Protocols describing what a controller can do with the hardware.

Two families of protocols are exposed:

* Drives —
  [DriveController][ctaops.sdk.controllers.protocols.drive.DriveController]
  and
  [MountedDriveController][ctaops.sdk.controllers.protocols.drive.MountedDriveController].
* Libraries —
  [LibraryController][ctaops.sdk.controllers.protocols.library.LibraryController]
  and
  [ManagedLibraryController][ctaops.sdk.controllers.protocols.library.ManagedLibraryController].

Concrete SCSI/udev-backed implementations live under
[ctaops.sdk.controllers.scsi][ctaops.sdk.controllers.scsi]. For convenience
every protocol is also re-exported from the top-level `ctaops.sdk`
namespace.
"""
