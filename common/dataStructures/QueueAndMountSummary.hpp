/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "MountType.hpp"
#include "MountPolicy.hpp"
#include "VidToTapeMap.hpp"

#include <string>
#include <list>

namespace cta::common::dataStructures {

/** This structure holds all the information regarding a VID (retrieves) */
struct QueueAndMountSummary {
  MountType mountType=MountType::NoMount;
  std::string tapePool;
  std::string vo;
  uint64_t readMaxDrives=0;
  uint64_t writeMaxDrives=0;
  std::string vid;
  std::string logicalLibrary;
  uint64_t filesQueued=0;
  uint64_t bytesQueued=0;
  time_t oldestJobAge=0;
  time_t youngestJobAge=0;
  MountPolicy mountPolicy;
  uint64_t currentMounts=0;
  uint64_t currentFiles=0;
  uint64_t currentBytes=0;
  double averageBandwidth=0;
  uint64_t tapesCapacity=0;
  uint64_t filesOnTapes=0;
  uint64_t dataOnTapes=0;
  uint64_t fullTapes=0;
  uint64_t writableTapes=0;
  struct SleepForSpaceInfo {
    time_t startTime;
    std::string diskSystemName;
    time_t sleepTime;
  };
  std::optional<SleepForSpaceInfo> sleepForSpaceInfo;
  std::string highestPriorityMountPolicy;
  std::string lowestRequestAgeMountPolicy;
  std::list<std::string> mountPolicies;

  static QueueAndMountSummary* getOrCreateEntry(std::list<QueueAndMountSummary> &summaryList,
    MountType mountType, const std::string &tapePool, const std::string &vid,
    const std::map<std::string, std::string, std::less<>> &vid_to_logical_library);
};

} // namespace cta::common::dataStructures
