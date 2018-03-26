/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
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

#include "MountType.hpp"
#include "MountPolicy.hpp"
#include "VidToTapeMap.hpp"

#include <string>
#include <list>

namespace cta {
namespace common {
namespace dataStructures {

/** This structure holds all the information regarding a VID (retrieves) */
struct QueueAndMountSummary {
  MountType mountType=MountType::NoMount;
  std::string tapePool;
  std::string vid;
  std::string logicalLibrary;
  uint64_t filesQueued=0;
  uint64_t bytesQueued=0;
  time_t oldestJobAge=0;
  MountPolicy mountPolicy;
  uint64_t currentMounts=0;
  uint64_t currentFiles=0;
  uint64_t currentBytes=0;
  double latestBandwidth=0;
  uint64_t nextMounts=0;
  uint64_t tapesCapacity=0;
  uint64_t filesOnTapes=0;
  uint64_t dataOnTapes=0;
  uint64_t fullTapes=0;
  uint64_t emptyTapes=0;
  uint64_t disabledTapes=0;
  uint64_t writableTapes=0;
  
  static QueueAndMountSummary &getOrCreateEntry(std::list<QueueAndMountSummary> &summaryList,
    MountType mountType, const std::string &tapePool, const std::string &vid,
    const common::dataStructures::VidToTapeMap &vid_to_tapeinfo);
};

}}} //namespace cta::common::dataStructures
