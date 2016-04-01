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

#include "common/dataStructures/WriteTestResult.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::WriteTestResult::WriteTestResult():
  noOfFilesWritten(0),
  totalBytesWritten(0),
  totalFilesWritten(0),
  totalTimeInSeconds(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::common::dataStructures::WriteTestResult::operator==(const WriteTestResult &rhs) const {
  return checksums==rhs.checksums
      && driveName==rhs.driveName
      && errors==rhs.errors
      && noOfFilesWritten==rhs.noOfFilesWritten
      && totalBytesWritten==rhs.totalBytesWritten
      && totalFilesWritten==rhs.totalFilesWritten
      && totalTimeInSeconds==rhs.totalTimeInSeconds
      && vid==rhs.vid;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool cta::common::dataStructures::WriteTestResult::operator!=(const WriteTestResult &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::WriteTestResult &obj) {
  os << "(checksums=" << obj.checksums
     << " driveName=" << obj.driveName
     << " errors=" << obj.errors
     << " noOfFilesWritten=" << obj.noOfFilesWritten
     << " totalBytesWritten=" << obj.totalBytesWritten
     << " totalFilesWritten=" << obj.totalFilesWritten
     << " totalTimeInSeconds=" << obj.totalTimeInSeconds
     << " vid=" << obj.vid << ")";
  return os;
}

