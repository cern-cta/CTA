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

#include <common/dataStructures/MountType.hpp>
#include "cta_admin.pb.h"

namespace cta::admin {

common::dataStructures::MountType ProtobufToMountType(MountType mountType) {
  using namespace common::dataStructures;

  switch(mountType) {
    case NO_MOUNT:              return common::dataStructures::MountType::NoMount;
    case ARCHIVE_FOR_USER:      return common::dataStructures::MountType::ArchiveForUser;
    case ARCHIVE_FOR_REPACK:    return common::dataStructures::MountType::ArchiveForRepack;
    case ARCHIVE_ALL_TYPES:     return common::dataStructures::MountType::ArchiveAllTypes;
    case RETRIEVE:              return common::dataStructures::MountType::Retrieve;
    case LABEL:                 return common::dataStructures::MountType::Label;
    case UNKNOWN_MOUNT_TYPE:
    default:
      throw std::runtime_error("In ProtobufToMountType(): unknown mount type " + std::to_string(mountType));
  }
}

MountType MountTypeToProtobuf(common::dataStructures::MountType mountType) {
  using namespace common::dataStructures;

  switch(mountType) {
    case common::dataStructures::MountType::NoMount:             return NO_MOUNT;
    case common::dataStructures::MountType::ArchiveForUser:      return ARCHIVE_FOR_USER;
    case common::dataStructures::MountType::ArchiveForRepack:    return ARCHIVE_FOR_REPACK;
    case common::dataStructures::MountType::ArchiveAllTypes:     return ARCHIVE_ALL_TYPES;
    case common::dataStructures::MountType::Retrieve:            return RETRIEVE;
    case common::dataStructures::MountType::Label:               return LABEL;
  }
  return UNKNOWN_MOUNT_TYPE;
}

} // namespace cta::admin
