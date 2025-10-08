/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/MountType.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

std::string toString(cta::common::dataStructures::MountType type) {
  switch(type) {
    case MountType::ArchiveForUser:
      return "ARCHIVE_FOR_USER";
    case MountType::ArchiveForRepack:
      return "ARCHIVE_FOR_REPACK";
    case MountType::ArchiveAllTypes:
      return "ARCHIVE_ALL_TYPES";
    case MountType::Retrieve:
      return "RETRIEVE";
    case MountType::Label:
      return "LABEL";
    case MountType::NoMount:
      return "NO_MOUNT";
    default:
      return "UNKNOWN";
  }
}

std::string toCamelCaseString(cta::common::dataStructures::MountType type) {
  switch(type) {
    case MountType::ArchiveForUser:
      return "ArchiveForUser";
    case MountType::ArchiveForRepack:
      return "ArchiveForRepack";
    case MountType::ArchiveAllTypes:
      return "ArchiveAllTypes";
    case MountType::Retrieve:
      return "Retrieve";
    case MountType::Label:
      return "Label";
    case MountType::NoMount:
      return "NoMount";
    default:
      return "Unknown";
  }
}

MountType strToMountType(const std::string& mountTypeStr) {
       if(mountTypeStr == "ARCHIVE_FOR_USER")   return MountType::ArchiveForUser;
  else if(mountTypeStr == "ARCHIVE_FOR_REPACK") return MountType::ArchiveForRepack;
  else if(mountTypeStr == "ARCHIVE_ALL_TYPES")  return MountType::ArchiveAllTypes;
  else if(mountTypeStr == "RETRIEVE")           return MountType::Retrieve;
  else if(mountTypeStr == "LABEL")              return MountType::Label;
  else if(mountTypeStr == "NO_MOUNT")           return MountType::NoMount;
  else throw cta::exception::Exception("Mount type " + mountTypeStr + " does not correspond to a valid mount type.");
}

MountType getMountBasicType(MountType type) {
  switch(type) {
    case MountType::ArchiveForUser:
    case MountType::ArchiveForRepack:
      return MountType::ArchiveAllTypes;
    default:
      return type;
  }
}

std::ostream & operator<<(std::ostream &os,
  const cta::common::dataStructures::MountType &obj) {
  return os << toString(obj);
}

} // namespace cta::common::dataStructures
