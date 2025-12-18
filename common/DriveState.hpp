/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "scheduler/MountType.hpp"

#include <stdint.h>
#include <string>

namespace cta::common {

enum DriveStatusToDecommission {
  Down,
  Up,
  Starting,  // This status allows drive register to represent drives committed
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
  bool up;         ///< Should the drive be up?
  bool forceDown;  ///< Should going down preempt an existig mount?
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

}  // namespace cta::common

#error This should have been decomissioned
