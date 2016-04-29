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

#include "common/dataStructures/VerifyInfo.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
VerifyInfo::VerifyInfo():
  totalFiles(0),
  totalSize(0),
  filesToVerify(0),
  filesFailed(0),
  filesVerified(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool VerifyInfo::operator==(const VerifyInfo &rhs) const {
  return vid==rhs.vid
      && tag==rhs.tag
      && totalFiles==rhs.totalFiles
      && totalSize==rhs.totalSize
      && filesToVerify==rhs.filesToVerify
      && filesFailed==rhs.filesFailed
      && filesVerified==rhs.filesVerified
      && verifyStatus==rhs.verifyStatus
      && creationLog==rhs.creationLog
      && errors==rhs.errors;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool VerifyInfo::operator!=(const VerifyInfo &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const VerifyInfo &obj) {
  os << "(vid=" << obj.vid
     << " tag=" << obj.tag
     << " totalFiles=" << obj.totalFiles
     << " totalSize=" << obj.totalSize
     << " filesToVerify=" << obj.filesToVerify
     << " filesFailed=" << obj.filesFailed
     << " filesVerified=" << obj.filesVerified
     << " verifyStatus=" << obj.verifyStatus
     << " creationLog=" << obj.creationLog
     << " errors=" << obj.errors << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
