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

#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RetrieveRequest::RetrieveRequest() : archiveFileID(0), isVerifyOnly(false) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool RetrieveRequest::operator==(const RetrieveRequest& rhs) const {
  return requester == rhs.requester && archiveFileID == rhs.archiveFileID && dstURL == rhs.dstURL &&
         diskFileInfo == rhs.diskFileInfo && creationLog == rhs.creationLog && isVerifyOnly == rhs.isVerifyOnly &&
         vid == rhs.vid;
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

}  // namespace dataStructures
}  // namespace common
}  // namespace cta
