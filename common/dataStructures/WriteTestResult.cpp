/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include "common/dataStructures/WriteTestResult.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

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

} // namespace dataStructures
} // namespace common
} // namespace cta
