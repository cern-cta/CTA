# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""SCSI/udev-backed implementations of the controller protocols.

See [ctaops.sdk.controllers.protocols][ctaops.sdk.controllers.protocols]
for the public contracts. The classes in this package drive the hardware
through `cta-smc`, `cta-tape-label`, `sg_logs`, and `mt`, and resolve
drive/library metadata through the local udev repositories under
[ctaops.sdk.repositories.local][ctaops.sdk.repositories.local].

Two families of controllers are exposed, one per protocol layer:

* Drives —
  [ScsiDriveController][ctaops.sdk.controllers.scsi.drive.ScsiDriveController]
  and
  [ScsiMountedDriveController][ctaops.sdk.controllers.scsi.drive.ScsiMountedDriveController].
* Libraries —
  [ScsiLibraryController][ctaops.sdk.controllers.scsi.library.ScsiLibraryController]
  and
  [ScsiManagedLibraryController][ctaops.sdk.controllers.scsi.library.ScsiManagedLibraryController].
"""
