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
