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

#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool DriveState::operator==(const DriveState& rhs) const {
  return driveName == rhs.driveName && host == rhs.host && logicalLibrary == rhs.logicalLibrary &&
         sessionId == rhs.sessionId && bytesTransferredInSession == rhs.bytesTransferredInSession &&
         filesTransferredInSession == rhs.filesTransferredInSession && sessionStartTime == rhs.sessionStartTime &&
         mountStartTime == rhs.mountStartTime && transferStartTime == rhs.transferStartTime &&
         unloadStartTime == rhs.unloadStartTime && unmountStartTime == rhs.unmountStartTime &&
         drainingStartTime == rhs.drainingStartTime && downOrUpStartTime == rhs.downOrUpStartTime &&
         probeStartTime == rhs.probeStartTime && cleanupStartTime == rhs.cleanupStartTime &&
         lastUpdateTime == rhs.lastUpdateTime && startStartTime == rhs.startStartTime &&
         shutdownTime == rhs.shutdownTime && mountType == rhs.mountType && driveStatus == rhs.driveStatus &&
         desiredDriveState == rhs.desiredDriveState && currentVid == rhs.currentVid &&
         currentTapePool == rhs.currentTapePool && currentVo == rhs.currentVo &&
         currentPriority == rhs.currentPriority && bool(currentActivity) == bool(rhs.currentActivity) &&
         (currentActivity ? (currentActivity.value() == rhs.currentActivity.value()) : true) &&
         nextMountType == rhs.nextMountType && nextTapepool == rhs.nextTapepool && nextVo == rhs.nextVo &&
         nextVid == rhs.nextVid && bool(nextActivity) == bool(rhs.nextActivity) &&
         (nextActivity ? (nextActivity.value() == rhs.nextActivity.value()) : true);
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool DriveState::operator!=(const DriveState& rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const DriveState& obj) {
  os << "(name=" << obj.driveName << " host=" << obj.host << " logicalLibrary=" << obj.logicalLibrary
     << " sessionId=" << obj.sessionId << " bytesTransferedInSession=" << obj.bytesTransferredInSession
     << " filesTransferedInSession=" << obj.filesTransferredInSession << " sessionStartTime=" << obj.sessionStartTime
     << " mountStartTime=" << obj.mountStartTime << " transferStartTime=" << obj.transferStartTime
     << " unloadStartTime=" << obj.unloadStartTime << " unmountStartTime=" << obj.unmountStartTime
     << " drainingStartTime=" << obj.drainingStartTime << " downOrUpStartTime=" << obj.downOrUpStartTime
     << " probeStartTime=" << obj.probeStartTime << " cleanupStartTime=" << obj.cleanupStartTime
     << " lastUpdateTime=" << obj.lastUpdateTime << " startStartTime=" << obj.startStartTime
     << " shutdownTime=" << obj.shutdownTime << " mountType=" << obj.mountType << " status=" << obj.driveStatus
     << " desiredState=" << obj.desiredDriveState << " currentVid=" << obj.currentVid
     << " currentTapePool=" << obj.currentTapePool << " currentVo=" << obj.currentVo
     << " currentPriority=" << obj.currentPriority << " currentActivity=";
  if (obj.currentActivity) {
    os << obj.currentActivity.value();
  }
  else {
    os << "(none)";
  }
  os << " nextMountType=" << obj.nextMountType << " nextVid=" << obj.nextVid << " nextTapePool=" << obj.nextTapepool
     << " nextVo=" << obj.nextVo << " currentNext=";
  if (obj.nextActivity) {
    os << obj.nextActivity.value();
  }
  else {
    os << "(none)";
  }
  os << ")";
  return os;
}

}  // namespace dataStructures
}  // namespace common
}  // namespace cta
