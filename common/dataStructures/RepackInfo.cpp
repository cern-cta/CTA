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

#include "common/dataStructures/RepackInfo.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::common::dataStructures::RepackInfo::operator==(const RepackInfo &rhs) const {
  return creationLog==rhs.creationLog
      && errors==rhs.errors
      && filesArchived==rhs.filesArchived
      && filesFailed==rhs.filesFailed
      && filesToArchive==rhs.filesToArchive
      && filesToRetrieve==rhs.filesToRetrieve
      && repackStatus==rhs.repackStatus
      && repackType==rhs.repackType
      && tag==rhs.tag
      && totalFiles==rhs.totalFiles
      && totalSize==rhs.totalSize
      && vid==rhs.vid;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool cta::common::dataStructures::RepackInfo::operator!=(const RepackInfo &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::RepackInfo &obj) {
  os << "(creationLog=" << obj.creationLog
     << " errors=" << obj.errors
     << " filesArchived=" << obj.filesArchived
     << " filesFailed=" << obj.filesFailed
     << " filesToArchive=" << obj.filesToArchive
     << " filesToRetrieve=" << obj.filesToRetrieve
     << " repackStatus=" << obj.repackStatus
     << " repackType=" << obj.repackType
     << " tag=" << obj.tag
     << " totalFiles=" << obj.totalFiles
     << " totalSize=" << obj.totalSize
     << " vid=" << obj.vid << ")";
  return os;
}

