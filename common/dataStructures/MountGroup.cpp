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

#include "common/dataStructures/MountGroup.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::common::dataStructures::MountGroup::operator==(const MountGroup &rhs) const {
  return archive_minBytesQueued==rhs.archive_minBytesQueued
      && archive_minFilesQueued==rhs.archive_minFilesQueued
      && archive_minRequestAge==rhs.archive_minRequestAge
      && archive_priority==rhs.archive_priority
      && comment==rhs.comment
      && creationLog==rhs.creationLog
      && lastModificationLog==rhs.lastModificationLog
      && maxDrivesAllowed==rhs.maxDrivesAllowed
      && name==rhs.name
      && retrieve_minBytesQueued==rhs.retrieve_minBytesQueued
      && retrieve_minFilesQueued==rhs.retrieve_minFilesQueued
      && retrieve_minRequestAge==rhs.retrieve_minRequestAge
      && retrieve_priority==rhs.retrieve_priority;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool cta::common::dataStructures::MountGroup::operator!=(const MountGroup &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::MountGroup &obj) {
  os << "(archive_minBytesQueued=" << obj.archive_minBytesQueued
     << " archive_minFilesQueued=" << obj.archive_minFilesQueued
     << " archive_minRequestAge=" << obj.archive_minRequestAge
     << " archive_priority=" << obj.archive_priority
     << " comment=" << obj.comment
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " maxDrivesAllowed=" << obj.maxDrivesAllowed
     << " name=" << obj.name
     << " retrieve_minBytesQueued=" << obj.retrieve_minBytesQueued
     << " retrieve_minFilesQueued=" << obj.retrieve_minFilesQueued
     << " retrieve_minRequestAge=" << obj.retrieve_minRequestAge
     << " retrieve_priority=" << obj.retrieve_priority << ")";
  return os;
}

