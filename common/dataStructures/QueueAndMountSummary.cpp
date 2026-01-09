/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/QueueAndMountSummary.hpp"

#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

QueueAndMountSummary*
QueueAndMountSummary::getOrCreateEntry(std::vector<QueueAndMountSummary>& summaryList,
                                       MountType mountType,
                                       const std::string& tapePool,
                                       const std::string& vid,
                                       const std::map<std::string, std::string, std::less<>>& vid_to_logical_library) {
  for (auto& summary : summaryList) {
    if ((summary.tapePool == tapePool && summary.mountType == mountType
         && getMountBasicType(mountType) == MountType::ArchiveAllTypes)
        || (summary.vid == vid && mountType == MountType::Retrieve)) {
      return &summary;
    }
  }
  switch (mountType) {
    case MountType::ArchiveForUser:
    case MountType::Retrieve:
    case MountType::ArchiveForRepack:
      summaryList.emplace_back();
      summaryList.back().mountType = mountType;
      summaryList.back().tapePool = tapePool;
      if (MountType::ArchiveForUser == mountType || MountType::ArchiveForRepack == mountType) {
        summaryList.back().vid = "";
        summaryList.back().logicalLibrary = "";
      } else {
        summaryList.back().vid = vid;
        summaryList.back().logicalLibrary = vid_to_logical_library.at(vid);
      }
      return &summaryList.back();

    case MountType::Label:
    case MountType::NoMount:
    case MountType::ArchiveAllTypes:
      throw exception::Exception("In QueueAndMountSummary::getOrCreateEntry(): Unexpected mount type "
                                 + toString(mountType));
  }
  // This exception indicates possible memory corruption
  throw exception::Exception("In QueueAndMountSummary::getOrCreateEntry(): Invalid enum value, mountType="
                             + std::to_string(static_cast<uint32_t>(mountType)));
}

}  // namespace cta::common::dataStructures
