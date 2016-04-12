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
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::RepackInfo::RepackInfo():
  totalFiles(0),
  totalSize(0),
  filesToRetrieve(0),
  filesToArchive(0),
  filesFailed(0),
  filesArchived(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::common::dataStructures::RepackInfo::operator==(const RepackInfo &rhs) const {
  return vid==rhs.vid
      && tag==rhs.tag
      && totalFiles==rhs.totalFiles
      && totalSize==rhs.totalSize
      && filesToRetrieve==rhs.filesToRetrieve
      && filesToArchive==rhs.filesToArchive
      && filesFailed==rhs.filesFailed
      && filesArchived==rhs.filesArchived
      && repackType==rhs.repackType
      && repackStatus==rhs.repackStatus
      && errors==rhs.errors
      && creationLog==rhs.creationLog;
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
  os << "(vid=" << obj.vid
     << " tag=" << obj.tag
     << " totalFiles=" << obj.totalFiles
     << " totalSize=" << obj.totalSize
     << " filesToRetrieve=" << obj.filesToRetrieve
     << " filesToArchive=" << obj.filesToArchive
     << " filesFailed=" << obj.filesFailed
     << " filesArchived=" << obj.filesArchived
     << " repackType=" << obj.repackType
     << " repackStatus=" << obj.repackStatus
     << " errors=" << obj.errors
     << " creationLog=" << obj.creationLog << ")";
  return os;
}

