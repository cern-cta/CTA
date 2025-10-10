/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
#include "common/log/LogContext.hpp"

namespace cta {

namespace rdbms {
class ConnPool;
class Login;
class Rset;
class Stmt;
}

namespace catalogue {

class RdbmsDriveStateCatalogue : public DriveStateCatalogue {
public:
  RdbmsDriveStateCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool);
  ~RdbmsDriveStateCatalogue() override = default;

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
  log::Logger &m_log;

  std::shared_ptr<rdbms::ConnPool> m_connPool;

  void settingSqlTapeDriveValues(cta::rdbms::Stmt *stmt, const common::dataStructures::TapeDrive &tapeDrive) const;

  common::dataStructures::TapeDrive gettingSqlTapeDriveValues(cta::rdbms::Rset* rset) const;
};

}} // namespace cta::catalogue
