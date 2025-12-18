/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <common/dataStructures/MountType.hpp>

#include "cta_admin.pb.h"

namespace cta::admin {

inline common::dataStructures::MountType ProtobufToMountType(MountType mountType) {
  using namespace common::dataStructures;

  switch (mountType) {
    case NO_MOUNT:
      return common::dataStructures::MountType::NoMount;
    case ARCHIVE_FOR_USER:
      return common::dataStructures::MountType::ArchiveForUser;
    case ARCHIVE_FOR_REPACK:
      return common::dataStructures::MountType::ArchiveForRepack;
    case ARCHIVE_ALL_TYPES:
      return common::dataStructures::MountType::ArchiveAllTypes;
    case RETRIEVE:
      return common::dataStructures::MountType::Retrieve;
    case LABEL:
      return common::dataStructures::MountType::Label;
    case UNKNOWN_MOUNT_TYPE:
    default:
      throw std::runtime_error("In ProtobufToMountType(): unknown mount type " + std::to_string(mountType));
  }
}

inline MountType MountTypeToProtobuf(common::dataStructures::MountType mountType) {
  using namespace common::dataStructures;

  switch (mountType) {
    case common::dataStructures::MountType::NoMount:
      return NO_MOUNT;
    case common::dataStructures::MountType::ArchiveForUser:
      return ARCHIVE_FOR_USER;
    case common::dataStructures::MountType::ArchiveForRepack:
      return ARCHIVE_FOR_REPACK;
    case common::dataStructures::MountType::ArchiveAllTypes:
      return ARCHIVE_ALL_TYPES;
    case common::dataStructures::MountType::Retrieve:
      return RETRIEVE;
    case common::dataStructures::MountType::Label:
      return LABEL;
  }
  return UNKNOWN_MOUNT_TYPE;
}

}  // namespace cta::admin
