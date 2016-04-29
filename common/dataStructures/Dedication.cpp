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

#include "common/dataStructures/Dedication.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Dedication::Dedication():
  fromTimestamp(0),
  untilTimestamp(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool Dedication::operator==(const Dedication &rhs) const {
  return driveName==rhs.driveName
      && dedicationType==rhs.dedicationType
      && tag==rhs.tag
      && vid==rhs.vid
      && fromTimestamp==rhs.fromTimestamp
      && untilTimestamp==rhs.untilTimestamp
      && creationLog==rhs.creationLog
      && lastModificationLog==rhs.lastModificationLog
      && comment==rhs.comment;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool Dedication::operator!=(const Dedication &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const Dedication &obj) {
  os << "(driveName=" << obj.driveName
     << " dedicationType=" << obj.dedicationType
     << " tag=" << obj.tag
     << " vid=" << obj.vid
     << " fromTimestamp=" << obj.fromTimestamp
     << " untilTimestamp=" << obj.untilTimestamp
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " comment=" << obj.comment << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
