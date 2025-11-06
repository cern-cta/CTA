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
 * Using RAII, this class ensures that the callback is removed when this class goes out of scope
 */
class DriveSessionTracker {
public:
  DriveSessionTracker(std::shared_ptr<cta::catalogue::Catalogue> catalogue, std::string_view driveName);
  ~DriveSessionTracker();
  DriveSessionTracker (const DriveSessionTracker&) = delete;
  DriveSessionTracker& operator= (const DriveSessionTracker&) = delete;

  std::optional<cta::common::dataStructures::TapeDrive> queryTapeDrive() const;

private:
  std::shared_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::string m_driveName;
};

}  // namespace castor::tape::tapeserver::daemon
