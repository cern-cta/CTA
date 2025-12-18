/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/RequesterActivityMountRule.hpp"

#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool RequesterActivityMountRule::operator==(const RequesterActivityMountRule& rhs) const {
  return diskInstance == rhs.diskInstance && name == rhs.name && activityRegex == rhs.activityRegex
         && mountPolicy == rhs.mountPolicy && creationLog == rhs.creationLog
         && lastModificationLog == rhs.lastModificationLog && comment == rhs.comment;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool RequesterActivityMountRule::operator!=(const RequesterActivityMountRule& rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const RequesterActivityMountRule& obj) {
  os << "(diskInstance=" << obj.diskInstance << " name=" << obj.name << " activityRegex=" << obj.activityRegex
     << " mountPolicy=" << obj.mountPolicy << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog << " comment=" << obj.comment << ")";
  return os;
}

}  // namespace cta::common::dataStructures
