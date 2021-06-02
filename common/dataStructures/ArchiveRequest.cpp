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

#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

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

} // namespace dataStructures
} // namespace common
} // namespace cta
