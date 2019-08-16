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
DiskFileInfo::DiskFileInfo() : owner_uid(0), gid(0) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DiskFileInfo::DiskFileInfo(const std::string &path, uint32_t owner_uid, uint32_t gid) :
  path(path), owner_uid(owner_uid), gid(gid) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool DiskFileInfo::operator==(const DiskFileInfo &rhs) const {
  return path==rhs.path
      && owner_uid==rhs.owner_uid
      && gid==rhs.gid;
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
     << " owner_uid=" << obj.owner_uid
     << " gid=" << obj.gid << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
