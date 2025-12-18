/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/SourcedParameter.hpp"
#include "tapeserver/daemon/common/TapedConfiguration.hpp"

#include <ctime>
#include <memory>
#include <string>

namespace cta {

namespace catalogue {
class Catalogue;
}

/**
 * Static class to set TapedConfiguration in Database
 */
class DriveConfig {
public:
  static void setTapedConfiguration(const cta::tape::daemon::common::TapedConfiguration& tapedConfiguration,
                                    catalogue::Catalogue* catalogue,
                                    const std::string& tapeDriveName);

private:
  static void
  checkConfigInDB(catalogue::Catalogue* catalogue, const std::string& tapeDriveName, const std::string& key);
  static void setConfigToDB(const cta::SourcedParameter<std::string>& sourcedParameter,
                            catalogue::Catalogue* catalogue,
                            const std::string& tapeDriveName);
  static void
  setConfigToDB(const cta::SourcedParameter<cta::tape::daemon::common::FetchReportOrFlushLimits>& sourcedParameter,
                catalogue::Catalogue* catalogue,
                const std::string& tapeDriveName);
  static void setConfigToDB(const cta::SourcedParameter<std::uint32_t>& sourcedParameter,
                            catalogue::Catalogue* catalogue,
                            const std::string& tapeDriveName);
  static void setConfigToDB(const cta::SourcedParameter<std::uint64_t>& sourcedParameter,
                            catalogue::Catalogue* catalogue,
                            const std::string& tapeDriveName);
  static void setConfigToDB(const cta::SourcedParameter<std::time_t>& sourcedParameter,
                            catalogue::Catalogue* catalogue,
                            const std::string& tapeDriveName);
  static void setConfigToDB(const cta::SourcedParameter<bool>& sourcedParameter,
                            catalogue::Catalogue* catalogue,
                            const std::string& tapeDriveName);
};  // class DriveConfig

}  // namespace cta
