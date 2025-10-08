/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/RequesterGroupMountRule.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool RequesterGroupMountRule::operator==(const RequesterGroupMountRule &rhs) const {
  return diskInstance==rhs.diskInstance
    && name==rhs.name
    && mountPolicy==rhs.mountPolicy
    && creationLog==rhs.creationLog
    && lastModificationLog==rhs.lastModificationLog
    && comment==rhs.comment;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool RequesterGroupMountRule::operator!=(const RequesterGroupMountRule &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const RequesterGroupMountRule &obj) {
  os << "(diskInstance=" << obj.diskInstance
     << " name=" << obj.name
     << " mountPolicy=" << obj.mountPolicy
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " comment=" << obj.comment << ")";
  return os;
}

} // namespace cta::common::dataStructures
