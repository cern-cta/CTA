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

#include "QueueAndMountSummary.hpp"
#include "common/exception/Exception.hpp"

#include <set>

namespace cta {
namespace common {
namespace dataStructures {

QueueAndMountSummary &QueueAndMountSummary::getOrCreateEntry(std::list<QueueAndMountSummary> &summaryList,
    MountType mountType, const std::string &tapePool, const std::string &vid,
    const std::map<std::string, std::string> &vid_to_logical_library)
{
  for (auto & summary: summaryList) {
    if((summary.tapePool == tapePool && summary.mountType == mountType && (getMountBasicType(mountType) == MountType::ArchiveAllTypes)) || (summary.vid == vid && mountType == MountType::Retrieve)) {
      return summary;
    }
  }
  if (std::set<MountType>({MountType::ArchiveForUser, MountType::Retrieve, MountType::ArchiveForRepack}).count(mountType)) {
    summaryList.push_back(QueueAndMountSummary());
    summaryList.back().mountType=mountType;
    summaryList.back().tapePool=tapePool;
    if (MountType::ArchiveForUser==mountType || MountType::ArchiveForRepack == mountType) {
      summaryList.back().vid="-";
      summaryList.back().logicalLibrary="-";
    } else {
      summaryList.back().vid=vid;
      summaryList.back().logicalLibrary=vid_to_logical_library.at(vid);
    }
    return summaryList.back();
  }
  throw cta::exception::Exception ("In QueueAndMountSummary::getOrCreateEntry(): Unexpected mount type.");
}

}}} //namespace cta::common::dataStructures
