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

#pragma once

#include "scheduler/MountType.hpp"
#include <string>
#include <stdint.h>

namespace cta { namespace common {

  enum DriveStatusToDecommission {
    Down,
    Up,
    Starting, // This status allows drive register to represent drives committed
    // to mounting a tape before the mounting is confirmed. It is necessary to
    // allow race-free scheduling
    Mounting,
    Transferring,
    Unloading,
    Unmounting,
    DrainingToDisk,
    CleaningUp
  };
  
  struct DesiredDriveStateToDecommission { 
    bool up;        ///< Should the drive be up?
    bool forceDown; ///< Should going down preempt an existig mount?
  };

  struct DriveStateToDecommission {
    std::string name;
    std::string logicalLibrary;
    uint64_t sessionId;
    uint64_t bytesTransferedInSession;
    uint64_t filesTransferedInSession;
    time_t sessionStartTime;
    time_t mountStartTime;
    time_t transferStartTime;
    time_t unloadStartTime;
    time_t unmountStartTime;
    time_t drainingStartTime;
    time_t downOrUpStartTime;
    time_t cleanupStartTime;
    time_t lastUpdateTime;
    time_t startStartTime;
    cta::MountTypeToDecommission::Enum mountType;
    dataStructures::DriveStatus status;
    std::string currentVid;
    std::string currentTapePool;
    dataStructures::DesiredDriveState desiredDriveState;
  };
  
}} //namespace cta::common

#error This should have been decomissioned
