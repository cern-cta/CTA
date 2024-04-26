/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <list>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>

#include "catalogue/interfaces/DriveStateCatalogue.hpp"
#include "common/log/Logger.hpp"

namespace cta::catalogue {

class DriveStateCatalogueRetryWrapper: public DriveStateCatalogue {
public:
  DriveStateCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue, log::Logger &m_log,
    const uint32_t maxTriesToConnect);

  ~DriveStateCatalogueRetryWrapper() override = default;

  void createTapeDrive(const common::dataStructures::TapeDrive &tapeDrive) override;

  std::list<std::string> getTapeDriveNames() const override;

  std::list<common::dataStructures::TapeDrive> getTapeDrives() const override;

  std::optional<common::dataStructures::TapeDrive> getTapeDrive(const std::string &tapeDriveName) const override;

  void setDesiredTapeDriveState(const std::string& tapeDriveName,
      const common::dataStructures::DesiredDriveState &desiredState) override;

  void setDesiredTapeDriveStateComment(const std::string& tapeDriveName,
    const std::string &comment) override;

  void updateTapeDriveStatistics(const std::string& tapeDriveName,
    const std::string& host, const std::string& logicalLibrary,
    const common::dataStructures::TapeDriveStatistics& statistics) override;

  void updateTapeDriveStatus(const common::dataStructures::TapeDrive &tapeDrive) override;

  void deleteTapeDrive(const std::string &tapeDriveName) override;

  std::map<std::string, uint64_t, std::less<>> getDiskSpaceReservations() const override;

  void reserveDiskSpace(const std::string& driveName, const uint64_t mountId,
    const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) override;

  void releaseDiskSpace(const std::string& driveName, const uint64_t mountId,
    const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) override;

private:
  const std::unique_ptr<Catalogue>& m_catalogue;
  log::Logger &m_log;
  uint32_t m_maxTriesToConnect;
};

} // namespace cta::catalogue
