/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/AdminUser.hpp"

#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool AdminUser::operator==(const AdminUser& rhs) const {
  return name == rhs.name && creationLog == rhs.creationLog && lastModificationLog == rhs.lastModificationLog
         && comment == rhs.comment;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool AdminUser::operator!=(const AdminUser& rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const AdminUser& obj) {
  os << "(name=" << obj.name << " creationLog=" << obj.creationLog << " lastModificationLog=" << obj.lastModificationLog
     << " comment=" << obj.comment << ")";
  return os;
}

}  // namespace cta::common::dataStructures
