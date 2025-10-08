/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
