/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/UserIdentity.hpp"

#include <limits>
#include <ostream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserIdentity::UserIdentity() throw():
  uid(std::numeric_limits<decltype(uid)>::max()),
  gid(std::numeric_limits<decltype(gid)>::max()) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserIdentity::UserIdentity(
  const uint32_t u,
  const uint32_t g) throw():
  uid(u),
  gid(g) {
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::UserIdentity::operator==(const UserIdentity &rhs) const {
  return uid == rhs.uid;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool cta::UserIdentity::operator!=(const UserIdentity &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const cta::UserIdentity &obj) {
  os << "(uid=" << obj.uid << " gid=" << obj.gid << ")";
  return os;
}
