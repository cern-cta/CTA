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

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/DriveStatus.hpp"
#include "common/dataStructures/MountType.hpp"

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

  std::string name;
  std::string host;
  std::string logicalLibrary;
  uint64_t sessionId;
  uint64_t bytesTransferedInSession;
  uint64_t filesTransferedInSession;
  uint64_t latestBandwidth;
  time_t sessionStartTime;
  time_t currentStateStartTime;
  MountType mountType;
  DriveStatus status;
  std::string currentVid;
  std::string currentTapePool;

}; // struct DriveState

std::ostream &operator<<(std::ostream &os, const DriveState &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
