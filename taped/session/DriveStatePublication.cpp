/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveStatePublication.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/TapeDrivesCatalogueState.hpp"
#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/exception/UserError.hpp"

namespace cta::tape::daemon {

void requestDriveDown(catalogue::Catalogue& catalogue,
                      const std::string& driveName,
                      common::dataStructures::DriveDownReason reason,
                      log::LogContext& lc,
                      std::string_view detail) {
  using namespace common::dataStructures;
  const auto current = catalogue.DriveState()->getTapeDrive(driveName);
  if (!current) {
    throw exception::UserError("Cannot request Down for missing drive: " + driveName);
  }

  DesiredDriveState desired;
  desired.reason = formatDriveDownReason(reason, detail);
  if (!current->desiredUp && current->reasonUpDown && !current->reasonUpDown->empty()
      && *current->reasonUpDown != formatDriveDownReason(DriveDownReason::Startup)
      && !isCleanDriveShutdownReason(*current->reasonUpDown)) {
    desired.reason = current->reasonUpDown;
  }

  // TODO: make desired-state updates atomic; a concurrent operator change can be overwritten here.
  TapeDrivesCatalogueState(catalogue).setDesiredDriveState(driveName, desired, lc);
}

}  // namespace cta::tape::daemon
