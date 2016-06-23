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

#include "common/dataStructures/DiskFileInfo.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DiskFileInfo::DiskFileInfo() {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool DiskFileInfo::operator==(const DiskFileInfo &rhs) const {
  return path==rhs.path
      && owner==rhs.owner
      && group==rhs.group
      && recoveryBlob==rhs.recoveryBlob;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool DiskFileInfo::operator!=(const DiskFileInfo &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const DiskFileInfo &obj) {
  os << "(path=" << obj.path
     << " owner=" << obj.owner
     << " group=" << obj.group
     << " recoveryBlob=" << obj.recoveryBlob << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
