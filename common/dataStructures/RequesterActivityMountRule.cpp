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

#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RequesterActivityMountRule::RequesterActivityMountRule() {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool RequesterActivityMountRule::operator==(const RequesterActivityMountRule &rhs) const {
  return diskInstance==rhs.diskInstance
    && name==rhs.name
    && activityRegex==rhs.activityRegex
    && mountPolicy==rhs.mountPolicy
    && creationLog==rhs.creationLog
    && lastModificationLog==rhs.lastModificationLog
    && comment==rhs.comment;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool RequesterActivityMountRule::operator!=(const RequesterActivityMountRule &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const RequesterActivityMountRule &obj) {
  os << "(diskInstance=" << obj.diskInstance
     << " name=" << obj.name
     << " activityRegex=" << obj.activityRegex
     << " mountPolicy=" << obj.mountPolicy
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " comment=" << obj.comment << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
