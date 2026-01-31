/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/DriveStatus.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/dataStructures/TapeDrive.hpp"

#include <memory>
#include <optional>
#include <string>

namespace castor::tape::tapeserver::daemon {

/**
 * The purpose of this class is to observe the session state of a given drive and emit corresponding metrics about this.
 * Using RAII, this class ensures that the callback is removed when this class goes out of scope
 */
class DriveSessionTracker {
public:
  DriveSessionTracker(std::shared_ptr<cta::catalogue::Catalogue> catalogue, std::string_view driveName);
  ~DriveSessionTracker();
  DriveSessionTracker(const DriveSessionTracker&) = delete;
  DriveSessionTracker& operator=(const DriveSessionTracker&) = delete;

  std::optional<cta::common::dataStructures::MountType> getCurrentMountType() const;
  std::optional<cta::common::dataStructures::DriveStatus> getCurrentDriveStatus() const;
  std::optional<cta::common::dataStructures::TapeDrive> getCurrentDriveState() const;
  const std::string& getDriveName() const;

private:
  std::shared_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::string m_driveName;
};

}  // namespace castor::tape::tapeserver::daemon
