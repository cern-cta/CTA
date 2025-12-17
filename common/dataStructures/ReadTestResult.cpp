/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/ReadTestResult.hpp"

#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ReadTestResult::ReadTestResult() {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool ReadTestResult::operator==(const ReadTestResult& rhs) const {
  return driveName == rhs.driveName && vid == rhs.vid && noOfFilesRead == rhs.noOfFilesRead && errors == rhs.errors
         && checksums == rhs.checksums && totalBytesRead == rhs.totalBytesRead && totalFilesRead == rhs.totalFilesRead
         && totalTimeInSeconds == rhs.totalTimeInSeconds;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool ReadTestResult::operator!=(const ReadTestResult& rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const ReadTestResult& obj) {
  os << "(driveName=" << obj.driveName << " vid=" << obj.vid << " noOfFilesRead=" << obj.noOfFilesRead
     << " errors=" << obj.errors << " checksums=" << obj.checksums << " totalBytesRead=" << obj.totalBytesRead
     << " totalFilesRead=" << obj.totalFilesRead << " totalTimeInSeconds=" << obj.totalTimeInSeconds << ")";
  return os;
}

}  // namespace cta::common::dataStructures
