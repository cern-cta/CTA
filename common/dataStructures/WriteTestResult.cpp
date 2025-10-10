/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
