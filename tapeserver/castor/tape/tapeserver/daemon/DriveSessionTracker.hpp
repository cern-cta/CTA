/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
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

#include <string>
#include <optional>
#include <memory>
#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/DriveStatus.hpp"
#include "common/dataStructures/MountType.hpp"

namespace castor::tape::tapeserver::daemon {

/**
 * The purpose of this class is to observe the session state of a given drive and emit corresponding metrics about this.
 * The existence of this class solves a few problems:
 * - The callback is removed when the session no longer exists (when this class goes out of scope)
 * - It keeps track of the previous state within the session which is necessary as we not only emit what the current state is,
 *   we also need to emit that the previous state is no longer active.
 */
class DriveSessionTracker {
public:
  DriveSessionTracker(std::shared_ptr<cta::catalogue::Catalogue> catalogue, std::string_view driveName);
  ~DriveSessionTracker();

  std::optional<cta::common::dataStructures::TapeDrive> queryTapeDrive() const;

  bool hasState() const;
  bool isSameState(cta::common::dataStructures::DriveStatus driveStatus, cta::common::dataStructures::MountType mountType) const;
  void setState(cta::common::dataStructures::DriveStatus driveStatus, cta::common::dataStructures::MountType mountType);
  std::optional<cta::common::dataStructures::DriveStatus> getDriveStatus() const;
  std::optional<cta::common::dataStructures::MountType> getMountType() const;

private:
  std::shared_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::string m_driveName;
  std::optional<cta::common::dataStructures::DriveStatus> m_driveStatus;
  std::optional<cta::common::dataStructures::MountType> m_mountType;
};

}  // namespace castor::tape::tapeserver::daemon
