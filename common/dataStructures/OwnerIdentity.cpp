/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include <limits>

#include "common/dataStructures/OwnerIdentity.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

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

} // namespace dataStructures
} // namespace common
} // namespace cta
