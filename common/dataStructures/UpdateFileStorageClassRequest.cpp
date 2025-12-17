/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/UpdateFileStorageClassRequest.hpp"

#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
UpdateFileStorageClassRequest::UpdateFileStorageClassRequest() {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool UpdateFileStorageClassRequest::operator==(const UpdateFileStorageClassRequest& rhs) const {
  return requester == rhs.requester && archiveFileID == rhs.archiveFileID && storageClass == rhs.storageClass
         && diskFileInfo == rhs.diskFileInfo;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool UpdateFileStorageClassRequest::operator!=(const UpdateFileStorageClassRequest& rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const UpdateFileStorageClassRequest& obj) {
  os << "(requester=" << obj.requester << " archiveFileID=" << obj.archiveFileID << " storageClass=" << obj.storageClass
     << " diskFileInfo=" << obj.diskFileInfo << ")";
  return os;
}

}  // namespace cta::common::dataStructures
