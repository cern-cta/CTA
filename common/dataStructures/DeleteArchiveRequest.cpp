/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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
      && archiveFileID==rhs.archiveFileID && address==rhs.address && diskFileId == rhs.diskFileId
      && diskFilePath == rhs.diskFilePath && recycleTime == rhs.recycleTime && diskInstance == rhs.diskInstance;
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
     << " archiveFileID=" << obj.archiveFileID 
     << " diskFileId=" << obj.diskFileId
     << " diskFilePath=" << obj.diskFilePath
     << " recycleTime=" << obj.recycleTime
     << " instanceName=" << obj.diskInstance
     << " address=" << (obj.address ? obj.address.value() : "null") <<")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
