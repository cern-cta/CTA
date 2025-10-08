/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/ArchiveRouteType.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/utils/utils.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ArchiveRoute::ArchiveRoute():
  copyNb(0), type(ArchiveRouteType::DEFAULT) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool ArchiveRoute::operator==(const ArchiveRoute &rhs) const {
  return storageClassName==rhs.storageClassName
      && copyNb==rhs.copyNb
      && type==rhs.type
      && tapePoolName==rhs.tapePoolName
      && creationLog==rhs.creationLog
      && lastModificationLog==rhs.lastModificationLog
      && comment==rhs.comment;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool ArchiveRoute::operator!=(const ArchiveRoute &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const ArchiveRoute &obj) {
  os << "(storageClassName=" << obj.storageClassName
     << " copyNb=" << obj.copyNb
     << " archiveRouteType=" << obj.type
     << " tapePoolName=" << obj.tapePoolName
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " comment=" << obj.comment << ")";
  return os;
}

} // namespace cta::common::dataStructures
