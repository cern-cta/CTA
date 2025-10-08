/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <optional>
#include <stdint.h>
#include <string>
#include <vector>

#include "DriveStatus.hpp"
#include "MountType.hpp"
#include "DesiredDriveState.hpp"

namespace cta::common::dataStructures {

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
  std::optional<std::string> currentActivity;
  MountType nextMountType = MountType::NoMount;
  std::string nextVid;
  std::string nextTapepool;
  std::string nextVo;
  uint64_t nextPriority = 0;
  std::optional<std::string> nextActivity;
  std::vector<DriveConfigItem> driveConfigItems;
  std::string devFileName;
  std::string rawLibrarySlot;
}; // struct DriveState

std::ostream &operator<<(std::ostream &os, const DriveState &obj);

} // namespace cta::common::dataStructures
