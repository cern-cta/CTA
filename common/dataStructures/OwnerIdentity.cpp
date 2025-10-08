/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <limits>

#include "common/dataStructures/OwnerIdentity.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OwnerIdentity::OwnerIdentity() :
  uid(std::numeric_limits<uid_t>::max()),
  gid(std::numeric_limits<gid_t>::max()) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OwnerIdentity::OwnerIdentity(uint32_t uid, uint32_t gid) : uid(uid), gid(gid) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool OwnerIdentity::operator==(const OwnerIdentity &rhs) const {
  return uid == rhs.uid && gid == rhs.gid;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool OwnerIdentity::operator!=(const OwnerIdentity &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const OwnerIdentity &obj) {
  os << "(uid=" << obj.uid
     << " gid=" << obj.gid << ")";
  return os;
}

} // namespace cta::common::dataStructures
