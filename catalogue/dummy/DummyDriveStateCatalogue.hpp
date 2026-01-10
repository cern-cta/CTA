/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/DriveStateCatalogue.hpp"
#include "common/dataStructures/TapeDrive.hpp"

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>

namespace cta::catalogue {

class DummyDriveStateCatalogue : public DriveStateCatalogue {
public:
  DummyDriveStateCatalogue() = default;
  ~DummyDriveStateCatalogue() override = default;

  void createTapeDrive(const common::dataStructures::TapeDrive& tapeDrive) override;

  std::vector<std::string> getTapeDriveNames() const override;

  std::vector<common::dataStructures::TapeDrive> getTapeDrives() const override;

  std::unordered_map<std::string, std::optional<uint64_t>> getTapeDriveMountIDs() const override;

  std::optional<common::dataStructures::TapeDrive> getTapeDrive(const std::string& tapeDriveName) const override;

  void setDesiredTapeDriveState(const std::string& tapeDriveName,
                                const common::dataStructures::DesiredDriveState& desiredState) override;

  void setDesiredTapeDriveStateComment(const std::string& tapeDriveName, const std::string& comment) override;

  void updateTapeDriveStatistics(const std::string& tapeDriveName,
                                 const std::string& host,
                                 const std::string& logicalLibrary,
                                 const common::dataStructures::TapeDriveStatistics& statistics) override;

  void updateTapeDriveStatus(const common::dataStructures::TapeDrive& tapeDrive) override;

  void deleteTapeDrive(const std::string& tapeDriveName) override;

  std::map<std::string, uint64_t, std::less<>> getDiskSpaceReservations() const override;

  void reserveDiskSpace(const std::string& driveName,
                        const uint64_t mountId,
                        const DiskSpaceReservationRequest& diskSpaceReservation,
                        log::LogContext& lc) override;

  void releaseDiskSpace(const std::string& driveName,
                        const uint64_t mountId,
                        const DiskSpaceReservationRequest& diskSpaceReservation,
                        log::LogContext& lc) override;

private:
  common::dataStructures::TapeDrive m_tapeDriveStatus;
};

}  // namespace cta::catalogue
