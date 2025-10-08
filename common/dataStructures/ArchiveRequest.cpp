/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ArchiveRequest::ArchiveRequest(): fileSize(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool ArchiveRequest::operator==(const ArchiveRequest &rhs) const {
  return requester==rhs.requester
      && diskFileID==rhs.diskFileID
      && srcURL==rhs.srcURL
      && fileSize==rhs.fileSize
      && checksumBlob==rhs.checksumBlob
      && storageClass==rhs.storageClass
      && diskFileInfo==rhs.diskFileInfo
      && archiveReportURL==rhs.archiveReportURL
      && archiveErrorReportURL == rhs.archiveErrorReportURL
      && creationLog==rhs.creationLog;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool ArchiveRequest::operator!=(const ArchiveRequest &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const ArchiveRequest &obj) {
  os << "(requester=" << obj.requester
     << " diskFileID=" << obj.diskFileID
     << " srcURL=" << obj.srcURL
     << " fileSize=" << obj.fileSize
     << " checksumBlob=" << obj.checksumBlob
     << " storageClass=" << obj.storageClass
     << " diskFileInfo=" << obj.diskFileInfo
     << " archiveReportURL=" << obj.archiveReportURL
     << " archiveErrorReportURL=" << obj.archiveErrorReportURL
     << " creationLog=" << obj.creationLog << ")";
  return os;
}

} // namespace cta::common::dataStructures
