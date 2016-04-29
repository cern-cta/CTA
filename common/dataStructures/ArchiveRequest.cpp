/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
ArchiveRequest::ArchiveRequest():
  fileSize(0),
  diskpoolThroughput(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool ArchiveRequest::operator==(const ArchiveRequest &rhs) const {
  return requester==rhs.requester
      && diskFileID==rhs.diskFileID
      && instance==rhs.instance
      && srcURL==rhs.srcURL
      && fileSize==rhs.fileSize
      && checksumType==rhs.checksumType
      && checksumValue==rhs.checksumValue
      && storageClass==rhs.storageClass
      && drData==rhs.drData
      && diskpoolName==rhs.diskpoolName
      && diskpoolThroughput==rhs.diskpoolThroughput
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
     << " instance=" << obj.instance
     << " srcURL=" << obj.srcURL
     << " fileSize=" << obj.fileSize
     << " checksumType=" << obj.checksumType
     << " checksumValue=" << obj.checksumValue
     << " storageClass=" << obj.storageClass
     << " drData=" << obj.drData
     << " diskpoolName=" << obj.diskpoolName
     << " diskpoolThroughput=" << obj.diskpoolThroughput
     << " creationLog=" << obj.creationLog << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
