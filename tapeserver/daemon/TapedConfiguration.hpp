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
#include <type_traits>
#include <limits>
#include "DriveConfiguration.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/exception/Exception.hpp"
#include "SourcedParameter.hpp"

namespace cta {
namespace tape {
namespace daemon {
/**
 * Class containing all the parameters needed by the watchdog process
 * to spawn a transfer session per drive.
 */
struct TapedConfiguration {
  static TapedConfiguration createFromCtaConf(
          cta::log::Logger &log = gDummyLogger);
  static TapedConfiguration createFromCtaConf(
          const std::string & generalConfigPath,
          cta::log::Logger & log = gDummyLogger);
  
  //----------------------------------------------------------------------------
  // The actual parameters:
  //----------------------------------------------------------------------------
  // Basics: tp config
  //----------------------------------------------------------------------------
  /// Path to the file describing the tape drives (TPCONFIG)
  SourcedParameter<std::string> tpConfigPath = decltype(tpConfigPath)
     ("taped" , "tpConfigPath", "/etc/cta/TPCONFIG", "Compile time default");
  /// Extracted drives configuration.
  std::map<std::string, DriveConfiguration> driveConfigs;
  //----------------------------------------------------------------------------
  // Watchdog: parameters for timeouts in various situations.
  //----------------------------------------------------------------------------
  /// Maximum time allowed to complete a single mount scheduling.
  SourcedParameter<time_t> wdScheduleMaxSecs = decltype(wdScheduleMaxSecs)
    ("taped", "WatchdogScheduleMaxSecs", 60, "Compile time default");
  /// Maximum time allowed to complete mount a tape.
  SourcedParameter<time_t> wdMountMaxSecs =decltype(wdMountMaxSecs)
    ("taped", "WatchdogMountMaxSecs", 900, "Compile time default");
  /// Maximum time allowed after mounting without a single tape block move
  SourcedParameter<time_t> wdNoBlockMoveMaxSecs = decltype(wdNoBlockMoveMaxSecs)
    ("taped", "WatchdogNoBlockMoveMaxSecs", 1800, "Compile time default");
  /// Time to wait after scheduling came up idle
  SourcedParameter<time_t> wdIdleSessionTimer = decltype(wdIdleSessionTimer)
    ("taped", "WatchdogNoBlockMoveMaxSecs", 10, "Compile time default");
  //----------------------------------------------------------------------------
  // The central storage access configuration
  //---------------------------------------------------------------------------- 
  /// URL of the object store.
  SourcedParameter<std::string> objectStoreURL = decltype(objectStoreURL)
    ("general", "ObjectStoreURL");
  /// URL of the file catalog
  SourcedParameter<std::string> fileCatalogURL = decltype(fileCatalogURL)
    ("general", "FileCatalogURL");
  
private:
  /** A private dummy logger which will simplify the implementation of the 
   * functions (just unconditionally log things). */
  static cta::log::DummyLogger gDummyLogger;
} ;
}
}
} // namespace cta::tape::daemon