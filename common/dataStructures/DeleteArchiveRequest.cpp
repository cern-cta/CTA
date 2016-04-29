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

#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DeleteArchiveRequest::DeleteArchiveRequest():
  archiveFileID(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool DeleteArchiveRequest::operator==(const DeleteArchiveRequest &rhs) const {
  return requester==rhs.requester
      && archiveFileID==rhs.archiveFileID;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool DeleteArchiveRequest::operator!=(const DeleteArchiveRequest &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const DeleteArchiveRequest &obj) {
  os << "(requester=" << obj.requester
     << " archiveFileID=" << obj.archiveFileID << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
