/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
bool DriveState::operator==(const DriveState &rhs) const {
  return driveName==rhs.driveName
      && host==rhs.host
      && logicalLibrary==rhs.logicalLibrary
      && sessionId==rhs.sessionId
      && bytesTransferredInSession==rhs.bytesTransferredInSession
      && filesTransferredInSession==rhs.filesTransferredInSession
      && latestBandwidth==rhs.latestBandwidth
      && sessionStartTime==rhs.sessionStartTime
      && mountStartTime==rhs.mountStartTime
      && transferStartTime==rhs.transferStartTime
      && unloadStartTime==rhs.unloadStartTime
      && unmountStartTime==rhs.unmountStartTime
      && drainingStartTime==rhs.drainingStartTime
      && downOrUpStartTime==rhs.downOrUpStartTime
      && probeStartTime==rhs.probeStartTime
      && cleanupStartTime==rhs.cleanupStartTime
      && lastUpdateTime==rhs.lastUpdateTime
      && startStartTime==rhs.startStartTime
      && shutdownTime==rhs.shutdownTime
      && mountType==rhs.mountType
      && driveStatus==rhs.driveStatus
      && desiredDriveState==rhs.desiredDriveState
      && currentVid==rhs.currentVid
      && currentTapePool==rhs.currentTapePool
      && currentVo == rhs.currentVo
      && currentPriority == rhs.currentPriority
      && bool(currentActivityAndWeight) == bool(rhs.currentActivityAndWeight)
      && (currentActivityAndWeight? (
        currentActivityAndWeight.value().activity 
          == rhs.currentActivityAndWeight.value().activity
        && currentActivityAndWeight.value().weight
          == rhs.currentActivityAndWeight.value().weight
         ): true)
      && nextMountType == rhs.nextMountType
      && nextTapepool == rhs.nextTapepool
      && nextVo == rhs.nextVo
      && nextVid == rhs.nextVid
      && bool(nextActivityAndWeight) == bool(rhs.nextActivityAndWeight)
      && (nextActivityAndWeight? (
        nextActivityAndWeight.value().activity 
          == rhs.nextActivityAndWeight.value().activity
        && nextActivityAndWeight.value().weight
          == rhs.nextActivityAndWeight.value().weight
         ): true);
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool DriveState::operator!=(const DriveState &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const DriveState &obj) {
  os << "(name=" << obj.driveName
     << " host=" << obj.host
     << " logicalLibrary=" << obj.logicalLibrary
     << " sessionId=" << obj.sessionId
     << " bytesTransferedInSession=" << obj.bytesTransferredInSession
     << " filesTransferedInSession=" << obj.filesTransferredInSession
     << " latestBandwidth=" << obj.latestBandwidth
     << " sessionStartTime=" << obj.sessionStartTime
     << " mountStartTime="  << obj.mountStartTime
     << " transferStartTime="  << obj.transferStartTime
     << " unloadStartTime="  << obj.unloadStartTime
     << " unmountStartTime="  << obj.unmountStartTime
     << " drainingStartTime="  << obj.drainingStartTime
     << " downOrUpStartTime="  << obj.downOrUpStartTime
     << " probeStartTime=" << obj.probeStartTime
     << " cleanupStartTime="  << obj.cleanupStartTime
     << " lastUpdateTime="  << obj.lastUpdateTime
     << " startStartTime="  << obj.startStartTime
     << " shutdownTime=" << obj.shutdownTime
     << " mountType=" << obj.mountType
     << " status=" << obj.driveStatus
     << " desiredState=" << obj.desiredDriveState
     << " currentVid=" << obj.currentVid
     << " currentTapePool=" << obj.currentTapePool
     << " currentVo=" << obj.currentVo
     << " currentPriority=" << obj.currentPriority
     << " currentActivity=";
  if (obj.currentActivityAndWeight) {
    os << "(" << obj.currentActivityAndWeight.value().activity
       << "," << obj.currentActivityAndWeight.value().weight << ")";
  } else {
    os << "(none)";
  }
  os << " nextMountType=" << obj.nextMountType
     << " nextVid=" << obj.nextVid
     << " nextTapePool=" << obj.nextTapepool
     << " nextVo=" << obj.nextVo
     << " currentNext=";
  if (obj.nextActivityAndWeight) {
    os << "(" << obj.nextActivityAndWeight.value().activity
       << "," << obj.nextActivityAndWeight.value().weight << ")";
  } else {
    os << "(none)";
  }
  os << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
