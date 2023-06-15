/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
OwnerIdentity::OwnerIdentity() : uid(std::numeric_limits<uid_t>::max()), gid(std::numeric_limits<gid_t>::max()) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OwnerIdentity::OwnerIdentity(uint32_t uid, uint32_t gid) : uid(uid), gid(gid) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool OwnerIdentity::operator==(const OwnerIdentity& rhs) const {
  return uid == rhs.uid && gid == rhs.gid;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool OwnerIdentity::operator!=(const OwnerIdentity& rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const OwnerIdentity& obj) {
  os << "(uid=" << obj.uid << " gid=" << obj.gid << ")";
  return os;
}

}  // namespace dataStructures
}  // namespace common
}  // namespace cta
