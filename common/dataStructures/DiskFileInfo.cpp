/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
