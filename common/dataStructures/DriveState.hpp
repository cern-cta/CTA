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

#include <stdint.h>
#include <string>

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

  DriveState();

  bool operator==(const DriveState &rhs) const;

  bool operator!=(const DriveState &rhs) const;

  std::string driveName;
  std::string host;
  std::string logicalLibrary;
  uint64_t sessionId;
  uint64_t bytesTransferredInSession;
  uint64_t filesTransferredInSession;
  double latestBandwidth; /** < Byte per seconds */
  time_t sessionStartTime;
  time_t mountStartTime;
  time_t transferStartTime;
  time_t unloadStartTime;
  time_t unmountStartTime;
  time_t drainingStartTime;
  time_t downOrUpStartTime;
  time_t probeStartTime;
  time_t cleanupStartTime;
  time_t lastUpdateTime;
  time_t startStartTime;
  time_t shutdownTime;
  MountType mountType;
  DriveStatus driveStatus;
  DesiredDriveState desiredDriveState;
  std::string currentVid;
  std::string currentTapePool;
  uint64_t currentPriority;
  struct ActivityAndWeight {
    std::string activity;
    double weight;
  };
  optional<ActivityAndWeight> currentActivityAndWeight;
  MountType nextMountType;
  std::string nextVid;
  std::string nextTapepool;
  uint64_t nextPriority;
  optional<ActivityAndWeight> nextActivityAndWeight;
}; // struct DriveState

std::ostream &operator<<(std::ostream &os, const DriveState &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
