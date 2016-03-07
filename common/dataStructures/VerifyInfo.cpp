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

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::VerifyInfo::~VerifyInfo() throw() {
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::common::dataStructures::VerifyInfo::operator==(const VerifyInfo &rhs) const {
  return creationLog==rhs.creationLog
      && errors==rhs.errors
      && filesFailed==rhs.filesFailed
      && filesToVerify==rhs.filesToVerify
      && filesVerified==rhs.filesVerified
      && tag==rhs.tag
      && totalFiles==rhs.totalFiles
      && totalSize==rhs.totalSize
      && verifyStatus==rhs.verifyStatus
      && vid==rhs.vid;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool cta::common::dataStructures::VerifyInfo::operator!=(const VerifyInfo &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::VerifyInfo &obj) {
  os << "(creationLog=" << obj.creationLog
     << " errors=" << obj.errors
     << " filesFailed=" << obj.filesFailed
     << " filesToVerify=" << obj.filesToVerify
     << " filesVerified=" << obj.filesVerified
     << " tag=" << obj.tag
     << " totalFiles=" << obj.totalFiles
     << " totalSize=" << obj.totalSize
     << " verifyStatus=" << obj.verifyStatus
     << " vid=" << obj.vid << ")";
  return os;
}

