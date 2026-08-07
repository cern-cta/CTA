/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::catalogue {

class Catalogue;

/**
 * Static class to set the scheduler backend name in Database.
 * This is a temporary solution for backward compatibility and should be removed in the next catalogue upgrade.
 */
class DriveConfig {
public:
  static void setSchedulerBackendName(Catalogue* catalogue,
                                      const std::string& schedulerBackendName,
                                      const std::string& tapeDriveName);

};  // class DriveConfig

}  // namespace cta::catalogue
