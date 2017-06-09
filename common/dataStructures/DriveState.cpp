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

#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DriveState::DriveState():
  sessionId(0),
  bytesTransferedInSession(0),
  filesTransferedInSession(0),
  latestBandwidth(0),
  sessionStartTime(0),
  mountStartTime(0),
  transferStartTime(0),
  unloadStartTime(0),
  unmountStartTime(0),
  drainingStartTime(0),
  downOrUpStartTime(0),
  cleanupStartTime(0),
  lastUpdateTime(0),
  startStartTime(0),
  mountType(dataStructures::MountType::NoMount),
  driveStatus(dataStructures::DriveStatus::Down),
  desiredDriveState({false, false}),
  nextMountType(dataStructures::MountType::NoMount) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool DriveState::operator==(const DriveState &rhs) const {
  return driveName==rhs.driveName
      && host==rhs.host
      && logicalLibrary==rhs.logicalLibrary
      && sessionId==rhs.sessionId
      && bytesTransferedInSession==rhs.bytesTransferedInSession
      && filesTransferedInSession==rhs.filesTransferedInSession
      && latestBandwidth==rhs.latestBandwidth
      && sessionStartTime==rhs.sessionStartTime
      && mountStartTime==rhs.mountStartTime
      && transferStartTime==rhs.transferStartTime
      && unloadStartTime==rhs.unloadStartTime
      && unmountStartTime==rhs.unmountStartTime
      && drainingStartTime==rhs.drainingStartTime
      && downOrUpStartTime==rhs.downOrUpStartTime
      && cleanupStartTime==rhs.cleanupStartTime
      && lastUpdateTime==rhs.lastUpdateTime
      && startStartTime==rhs.startStartTime
      && mountType==rhs.mountType
      && driveStatus==rhs.driveStatus
      && desiredDriveState==rhs.desiredDriveState
      && currentVid==rhs.currentVid
      && currentTapePool==rhs.currentTapePool
      && nextMountType == rhs.nextMountType
      && nextTapepool == rhs.nextTapepool
      && nextVid == rhs.nextVid;
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
  return
  os << "(name=" << obj.driveName
     << " host=" << obj.host
     << " logicalLibrary=" << obj.logicalLibrary
     << " sessionId=" << obj.sessionId
     << " bytesTransferedInSession=" << obj.bytesTransferedInSession
     << " filesTransferedInSession=" << obj.filesTransferedInSession
     << " latestBandwidth=" << obj.latestBandwidth
     << " sessionStartTime=" << obj.sessionStartTime
     << " mountStartTime="  << obj.mountStartTime
     << " transferStartTime="  << obj.transferStartTime
     << " unloadStartTime="  << obj.unloadStartTime
     << " unmountStartTime="  << obj.unmountStartTime
     << " drainingStartTime="  << obj.drainingStartTime
     << " downOrUpStartTime="  << obj.downOrUpStartTime
     << " cleanupStartTime="  << obj.cleanupStartTime
     << " lastUpdateTime="  << obj.lastUpdateTime
     << " startStartTime="  << obj.startStartTime
     << " mountType=" << obj.mountType
     << " status=" << obj.driveStatus
     << " desiredState=" << obj.desiredDriveState
     << " currentVid=" << obj.currentVid
     << " currentTapePool=" << obj.currentTapePool
     << " nextMountType=" << obj.nextMountType
     << " nextVid=" << obj.nextVid
     << " nextTapePool=" << obj.nextTapepool << ")";
}

} // namespace dataStructures
} // namespace common
} // namespace cta
