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

#include <stdint.h>
#include <string>
#include <vector>

#include "DriveStatus.hpp"
#include "MountType.hpp"
#include "DesiredDriveState.hpp"
#include "common/optional.hpp"

namespace cta {
namespace common {
namespace dataStructures {

/**
 * This struct holds all drive state information, used to display the equivalent 
 * of the old "showqueues -x -D" CASTOR command 
 */
struct DriveState {

  bool operator==(const DriveState &rhs) const;

  bool operator!=(const DriveState &rhs) const;
  
  struct DriveConfigItem{
    std::string category;
    std::string key;
    std::string value;
    std::string source;
  };

  std::string driveName;
  std::string host;
  std::string logicalLibrary;
  std::string ctaVersion;
  uint64_t sessionId = 0;
  uint64_t bytesTransferredInSession = 0;
  uint64_t filesTransferredInSession = 0;
  time_t sessionStartTime = 0;
  time_t mountStartTime = 0;
  time_t transferStartTime = 0;
  time_t unloadStartTime = 0;
  time_t unmountStartTime = 0;
  time_t drainingStartTime = 0;
  time_t downOrUpStartTime = 0;
  time_t probeStartTime = 0;
  time_t cleanupStartTime = 0;
  time_t lastUpdateTime = 0;
  time_t startStartTime = 0;
  time_t shutdownTime = 0;
  MountType mountType = MountType::NoMount;
  DriveStatus driveStatus = DriveStatus::Down;
  DesiredDriveState desiredDriveState;
  std::string currentVid;
  std::string currentTapePool;
  std::string currentVo;
  uint64_t currentPriority = 0;
  optional<std::string> currentActivity;
  MountType nextMountType = MountType::NoMount;
  std::string nextVid;
  std::string nextTapepool;
  std::string nextVo;
  uint64_t nextPriority = 0;
  optional<std::string> nextActivity;
  std::vector<DriveConfigItem> driveConfigItems;
  std::string devFileName;
  std::string rawLibrarySlot;
}; // struct DriveState

std::ostream &operator<<(std::ostream &os, const DriveState &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
