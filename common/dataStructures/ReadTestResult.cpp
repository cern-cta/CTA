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

#include "common/dataStructures/ReadTestResult.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ReadTestResult::ReadTestResult():
  noOfFilesRead(0),
  totalBytesRead(0),
  totalFilesRead(0),
  totalTimeInSeconds(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool ReadTestResult::operator==(const ReadTestResult &rhs) const {
  return driveName==rhs.driveName
      && vid==rhs.vid
      && noOfFilesRead==rhs.noOfFilesRead
      && errors==rhs.errors
      && checksums==rhs.checksums
      && totalBytesRead==rhs.totalBytesRead
      && totalFilesRead==rhs.totalFilesRead
      && totalTimeInSeconds==rhs.totalTimeInSeconds;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool ReadTestResult::operator!=(const ReadTestResult &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const ReadTestResult &obj) {
  os << "(driveName=" << obj.driveName
     << " vid=" << obj.vid
     << " noOfFilesRead=" << obj.noOfFilesRead
     << " errors=" << obj.errors
     << " checksums=" << obj.checksums
     << " totalBytesRead=" << obj.totalBytesRead
     << " totalFilesRead=" << obj.totalFilesRead
     << " totalTimeInSeconds=" << obj.totalTimeInSeconds << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
