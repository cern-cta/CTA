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

#include "common/dataStructures/WriteTestResult.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
WriteTestResult::WriteTestResult():
  noOfFilesWritten(0),
  totalBytesWritten(0),
  totalFilesWritten(0),
  totalTimeInSeconds(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool WriteTestResult::operator==(const WriteTestResult &rhs) const {
  return driveName==rhs.driveName
      && vid==rhs.vid
      && noOfFilesWritten==rhs.noOfFilesWritten
      && errors==rhs.errors
      && checksums==rhs.checksums
      && totalBytesWritten==rhs.totalBytesWritten
      && totalFilesWritten==rhs.totalFilesWritten
      && totalTimeInSeconds==rhs.totalTimeInSeconds;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool WriteTestResult::operator!=(const WriteTestResult &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const WriteTestResult &obj) {
  os << "(driveName=" << obj.driveName
     << " vid=" << obj.vid
     << " noOfFilesWritten=" << obj.noOfFilesWritten
     << " errors=" << obj.errors
     << " checksums=" << obj.checksums
     << " totalBytesWritten=" << obj.totalBytesWritten
     << " totalFilesWritten=" << obj.totalFilesWritten
     << " totalTimeInSeconds=" << obj.totalTimeInSeconds << ")";
  return os;
}

} // namespace cta::common::dataStructures
