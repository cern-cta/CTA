/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019 CERN
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

#include "common/dataStructures/RequesterIdentity.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RequesterIdentity::RequesterIdentity() { }


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RequesterIdentity::RequesterIdentity(const std::string& name, const std::string& group):
  name(name), group(group) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool RequesterIdentity::operator==(const RequesterIdentity &rhs) const {
  return name==rhs.name
      && group==rhs.group;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool RequesterIdentity::operator!=(const RequesterIdentity &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const RequesterIdentity &obj) {
  os << "(name=" << obj.name
     << " group=" << obj.group << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
