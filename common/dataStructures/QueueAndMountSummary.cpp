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

#include "QueueAndMountSummary.hpp"
#include "common/exception/Exception.hpp"

#include <set>

namespace cta {
namespace common {
namespace dataStructures {

QueueAndMountSummary &QueueAndMountSummary::getOrCreateEntry(std::list<QueueAndMountSummary> &summaryList,
    MountType mountType, const std::string &tapePool, const std::string &vid,
    const common::dataStructures::VidToTapeMap &vid_to_tapeinfo)
{
  for (auto & summary: summaryList) {
    if ((mountType==MountType::Archive && summary.tapePool==tapePool) ||
        (mountType==MountType::Retrieve && summary.vid==vid))
      return summary;
  }
  if (std::set<MountType>({MountType::Archive, MountType::Retrieve}).count(mountType)) {
    summaryList.push_back(QueueAndMountSummary());
    summaryList.back().mountType=mountType;
    summaryList.back().tapePool=tapePool;
    if (MountType::Archive==mountType) {
      summaryList.back().vid="-";
      summaryList.back().logicalLibrary="-";
    } else {
      summaryList.back().vid=vid;
      summaryList.back().logicalLibrary=vid_to_tapeinfo.at(vid).logicalLibraryName;
    }
    return summaryList.back();
  }
  throw cta::exception::Exception ("In QueueAndMountSummary::getOrCreateEntry(): Unexpected mount type.");
}

}}} //namespace cta::common::dataStructures
