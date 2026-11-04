# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Protocols describing what a controller can do with the hardware.

Two families of protocols are exposed:

* Drives —
  [DriveController][cta.tapeserver.sdk.controllers.protocols.drive.DriveController]
  and
  [MountedDriveController][cta.tapeserver.sdk.controllers.protocols.drive.MountedDriveController].
* Libraries —
  [LibraryController][cta.tapeserver.sdk.controllers.protocols.library.LibraryController]
  and
  [ManagedLibraryController][cta.tapeserver.sdk.controllers.protocols.library.ManagedLibraryController].

Concrete SCSI/udev-backed implementations live under
[cta.tapeserver.sdk.controllers.scsi][cta.tapeserver.sdk.controllers.scsi]. For convenience
every protocol is also re-exported from the top-level `cta.tapeserver.sdk`
namespace.
"""
