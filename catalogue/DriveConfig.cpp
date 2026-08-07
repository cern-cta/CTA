/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveConfig.hpp"

#include "Catalogue.hpp"

namespace cta::catalogue {

void DriveConfig::setSchedulerBackendName(Catalogue* catalogue,
                                          const std::string& schedulerBackendName,
                                          const std::string& tapeDriveName) {
  // Temporary for backward compatibility,. Should be removed in the next catalogue upgrade
  catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName,
                                                  "general",
                                                  "SchedulerBackendName",
                                                  schedulerBackendName,
                                                  "Config");
}

}  // namespace cta::catalogue
