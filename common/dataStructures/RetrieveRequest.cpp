/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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
RetrieveRequest::RetrieveRequest(): archiveFileID(0), isVerifyOnly(false) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool RetrieveRequest::operator==(const RetrieveRequest &rhs) const {
  return requester==rhs.requester
      && archiveFileID==rhs.archiveFileID
      && dstURL==rhs.dstURL
      && diskFileInfo==rhs.diskFileInfo
      && creationLog==rhs.creationLog
      && isVerifyOnly==rhs.isVerifyOnly
      && vid==rhs.vid;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool RetrieveRequest::operator!=(const RetrieveRequest &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const RetrieveRequest &obj) {
  os << "(requester=" << obj.requester
     << " archiveFileID=" << obj.archiveFileID
     << " dstURL=" << obj.dstURL
     << " diskFileInfo=" << obj.diskFileInfo
     << " creationLog=" << obj.creationLog
     << " isVerifyOnly=" << obj.isVerifyOnly;
  if(obj.vid) os << " vid=" << *(obj.vid);
  os << ")";
  return os;
}


void RetrieveRequest::appendFileSizeToDstURL(const uint64_t fileSize) {
  cta::utils::appendParameterXRootFileURL(dstURL,"oss.asize",std::to_string(fileSize));
}

} // namespace dataStructures
} // namespace common
} // namespace cta
