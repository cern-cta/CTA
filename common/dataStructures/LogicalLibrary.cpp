  /*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
LogicalLibrary::LogicalLibrary(): isDisabled(false) {
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool LogicalLibrary::operator==(const LogicalLibrary &rhs) const {
  return name==rhs.name
      && creationLog==rhs.creationLog
      && lastModificationLog==rhs.lastModificationLog
      && comment==rhs.comment
      && disabledReason == rhs.disabledReason;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool LogicalLibrary::operator!=(const LogicalLibrary &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const LogicalLibrary &obj) {
  os << "(name=" << obj.name
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " comment=" << obj.comment
     << ")";
  return os;
}

} // namespace cta::common::dataStructures
