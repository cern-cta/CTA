/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/MountType.hpp"

#include <stdexcept>
#include <string_view>

namespace cta {

/// Local policy for new mounts; existing mounts still count towards scheduling limits.
enum class MountSelectionMode { All, ArchiveOnly, RetrieveOnly };

inline MountSelectionMode mountSelectionModeFromString(std::string_view mode) {
  if (mode == "all") {
    return MountSelectionMode::All;
  }
  if (mode == "archive_only") {
    return MountSelectionMode::ArchiveOnly;
  }
  if (mode == "retrieve_only") {
    return MountSelectionMode::RetrieveOnly;
  }
  throw std::invalid_argument("Mount mode must be one of [all, archive_only, retrieve_only]");
}

/// Repack writes follow archive policy; repack reads follow retrieve policy.
inline bool allowsMount(MountSelectionMode mode, common::dataStructures::MountType type) {
  switch (mode) {
    case MountSelectionMode::All:
      return true;
    case MountSelectionMode::ArchiveOnly:
      return common::dataStructures::getMountBasicType(type) == common::dataStructures::MountType::ArchiveAllTypes;
    case MountSelectionMode::RetrieveOnly:
      return type == common::dataStructures::MountType::Retrieve;
  }
  return false;
}

}  // namespace cta
