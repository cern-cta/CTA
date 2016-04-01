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

#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::LogicalLibrary::LogicalLibrary():
  archiveMinBytesQueued(0),
  retrieveMinBytesQueued(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::common::dataStructures::LogicalLibrary::operator==(const LogicalLibrary &rhs) const {
  return archiveMinBytesQueued==rhs.archiveMinBytesQueued
      && comment==rhs.comment
      && creationLog==rhs.creationLog
      && lastModificationLog==rhs.lastModificationLog
      && name==rhs.name
      && retrieveMinBytesQueued==rhs.retrieveMinBytesQueued;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool cta::common::dataStructures::LogicalLibrary::operator!=(const LogicalLibrary &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::LogicalLibrary &obj) {
  os << "(archiveMinBytesQueued=" << obj.archiveMinBytesQueued
     << " comment=" << obj.comment
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " name=" << obj.name
     << " retrieveMinBytesQueued=" << obj.retrieveMinBytesQueued << ")";
  return os;
}

