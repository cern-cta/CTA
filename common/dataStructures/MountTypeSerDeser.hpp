/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Convert common::dataStructures::MountType to/from admin::DriveLsItem::MountType
 * @copyright      Copyright 2019 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <common/dataStructures/MountType.hpp>
#include "cta_admin.pb.h"

namespace cta {
namespace admin {

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

}} // cta::admin
