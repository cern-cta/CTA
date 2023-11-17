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

#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

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

} // namespace cta::common::dataStructures
