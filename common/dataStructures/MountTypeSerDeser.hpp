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

common::dataStructures::MountType ProtobufToMountType(DriveLsItem::MountType mountType) {
  using namespace common::dataStructures;

  switch(mountType) {
    case DriveLsItem::NO_MOUNT:              return MountType::NoMount;
    case DriveLsItem::ARCHIVE_FOR_USER:      return MountType::ArchiveForUser;
    case DriveLsItem::ARCHIVE_FOR_REPACK:    return MountType::ArchiveForRepack;
    case DriveLsItem::ARCHIVE_ALL_TYPES:     return MountType::ArchiveAllTypes;
    case DriveLsItem::RETRIEVE:              return MountType::Retrieve;
    case DriveLsItem::LABEL:                 return MountType::Label;
    case DriveLsItem::UNKNOWN_MOUNT_TYPE:
    default:
      throw std::runtime_error("In ProtobufToMountType(): unknown mount type " + std::to_string(mountType));
  }
}

DriveLsItem::MountType MountTypeToProtobuf(common::dataStructures::MountType mountType) {
  using namespace common::dataStructures;

  switch(mountType) {
    case MountType::NoMount:                 return DriveLsItem::NO_MOUNT;
    case MountType::ArchiveForUser:          return DriveLsItem::ARCHIVE_FOR_USER;
    case MountType::ArchiveForRepack:        return DriveLsItem::ARCHIVE_FOR_REPACK;
    case MountType::ArchiveAllTypes:         return DriveLsItem::ARCHIVE_ALL_TYPES;
    case MountType::Retrieve:                return DriveLsItem::RETRIEVE;
    case MountType::Label:                   return DriveLsItem::LABEL;
  }
  return DriveLsItem::UNKNOWN_MOUNT_TYPE;
}

}} // cta::admin
