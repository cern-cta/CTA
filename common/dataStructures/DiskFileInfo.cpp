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

#include "common/dataStructures/DiskFileInfo.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

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

} // namespace cta::common::dataStructures
