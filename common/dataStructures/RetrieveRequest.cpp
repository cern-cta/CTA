/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/RetrieveRequest.hpp"

#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RetrieveRequest::RetrieveRequest() {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool RetrieveRequest::operator==(const RetrieveRequest& rhs) const {
  return requester == rhs.requester && archiveFileID == rhs.archiveFileID && dstURL == rhs.dstURL
         && diskFileInfo == rhs.diskFileInfo && creationLog == rhs.creationLog && isVerifyOnly == rhs.isVerifyOnly
         && vid == rhs.vid;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool RetrieveRequest::operator!=(const RetrieveRequest& rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const RetrieveRequest& obj) {
  os << "(requester=" << obj.requester << " archiveFileID=" << obj.archiveFileID << " dstURL=" << obj.dstURL
     << " diskFileInfo=" << obj.diskFileInfo << " creationLog=" << obj.creationLog
     << " isVerifyOnly=" << obj.isVerifyOnly;
  if (obj.vid) {
    os << " vid=" << *(obj.vid);
  }
  os << ")";
  return os;
}

void RetrieveRequest::appendFileSizeToDstURL(const uint64_t fileSize) {
  cta::utils::appendParameterXRootFileURL(dstURL, "oss.asize", std::to_string(fileSize));
}

}  // namespace cta::common::dataStructures
