/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
StorageClass::StorageClass():
  nbCopies(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool StorageClass::operator==(const StorageClass &rhs) const {
  return name==rhs.name;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool StorageClass::operator!=(const StorageClass &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const StorageClass &obj) {
  os << "(name=" << obj.name
     << " nbCopies=" << obj.nbCopies
     << " vo=" << obj.vo.name
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " comment=" << obj.comment << ")";
  return os;
}

} // namespace cta::common::dataStructures
