/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once
#include <string>
#include <map>
#include "DriveConfiguration.hpp"
#include "common/log/DummyLogger.hpp"

namespace cta {
namespace tape {
namespace daemon {
/**
 * Class containing all the parameters needed by the watchdog process
 * to spawn a transfer session per drive.
 */
struct GlobalConfiguration {
  static GlobalConfiguration createFromCtaConf(
          cta::log::Logger &log = gDummyLogger);
  static GlobalConfiguration createFromCtaConf(
          const std::string & generalConfigPath,
          const std::string & tapeConfigFile,
          cta::log::Logger & log = gDummyLogger);
  std::map<std::string, DriveConfiguration> driveConfigs;
private:
  /** A private dummy logger which will simplify the implementaion of the 
   * functions (just unconditionally log things). */
  static cta::log::DummyLogger gDummyLogger;
} ;
}
}
} // namespace cta::tape::daemon