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

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::common::dataStructures::Dedication::operator==(const Dedication &rhs) const {
  return comment==rhs.comment
      && creationLog==rhs.creationLog
      && dedicationType==rhs.dedicationType
      && driveName==rhs.driveName
      && fromTimestamp==rhs.fromTimestamp
      && lastModificationLog==rhs.lastModificationLog
      && mountGroup==rhs.mountGroup
      && tag==rhs.tag
      && untilTimestamp==rhs.untilTimestamp
      && vid==rhs.vid;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool cta::common::dataStructures::Dedication::operator!=(const Dedication &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::Dedication &obj) {
  os << "(comment=" << obj.comment
     << " creationLog=" << obj.creationLog
     << " dedicationType=" << obj.dedicationType
     << " driveName=" << obj.driveName
     << " fromTimestamp=" << obj.fromTimestamp
     << " lastModificationLog=" << obj.lastModificationLog
     << " mountGroup=" << obj.mountGroup
     << " tag=" << obj.tag
     << " untilTimestamp=" << obj.untilTimestamp
     << " vid=" << obj.vid << ")";
  return os;
}

