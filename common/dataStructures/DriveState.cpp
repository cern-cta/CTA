/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

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
      && bool(currentActivity) == bool(rhs.currentActivity)
      && (currentActivity? (
        currentActivity.value() == rhs.currentActivity.value()
         ): true)
      && nextMountType == rhs.nextMountType
      && nextTapepool == rhs.nextTapepool
      && nextVo == rhs.nextVo
      && nextVid == rhs.nextVid
      && bool(nextActivity) == bool(rhs.nextActivity)
      && (nextActivity? (
        nextActivity.value() == rhs.nextActivity.value()
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
  if (obj.currentActivity) {
    os << obj.currentActivity.value();
  } else {
    os << "(none)";
  }
  os << " nextMountType=" << obj.nextMountType
     << " nextVid=" << obj.nextVid
     << " nextTapePool=" << obj.nextTapepool
     << " nextVo=" << obj.nextVo
     << " currentNext=";
  if (obj.nextActivity) {
    os << obj.nextActivity.value();
  } else {
    os << "(none)";
  }
  os << ")";
  return os;
}

} // namespace cta::common::dataStructures
