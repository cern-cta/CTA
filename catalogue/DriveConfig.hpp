/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <ctime>
#include <memory>
#include <string>

#include "common/SourcedParameter.hpp"
#include "tapeserver/daemon/common/TapedConfiguration.hpp"

namespace cta {

namespace catalogue {
class Catalogue;
}

/**
 * Static class to set TapedConfiguration in Database
 */
class DriveConfig {
 public:
  static void setTapedConfiguration(const cta::tape::daemon::common::TapedConfiguration &tapedConfiguration,
    catalogue::Catalogue* catalogue, const std::string& tapeDriveName);

 private:
  static void checkConfigInDB(catalogue::Catalogue* catalogue, const std::string& tapeDriveName,
    const std::string& key);
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
};  // class DriveConfig

} // namespace cta
