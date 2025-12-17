/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/CancelRetrieveRequest.hpp"

#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CancelRetrieveRequest::CancelRetrieveRequest() {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool CancelRetrieveRequest::operator==(const CancelRetrieveRequest& rhs) const {
  return requester == rhs.requester && archiveFileID == rhs.archiveFileID && dstURL == rhs.dstURL
         && diskFileInfo == rhs.diskFileInfo;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool CancelRetrieveRequest::operator!=(const CancelRetrieveRequest& rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const CancelRetrieveRequest& obj) {
  os << "(requester=" << obj.requester << " archiveFileID=" << obj.archiveFileID << " dstURL=" << obj.dstURL
     << " diskFileInfo=" << obj.diskFileInfo << ")";
  return os;
}

}  // namespace cta::common::dataStructures
