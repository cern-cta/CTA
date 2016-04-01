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

#include "common/dataStructures/Tape.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::Tape::Tape():
  capacityInBytes(0),
  dataOnTapeInBytes(0),
  lastFSeq(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::common::dataStructures::Tape::operator==(const Tape &rhs) const {
  return busy==rhs.busy
      && capacityInBytes==rhs.capacityInBytes
      && comment==rhs.comment
      && creationLog==rhs.creationLog
      && dataOnTapeInBytes==rhs.dataOnTapeInBytes
      && disabled==rhs.disabled
      && encryptionKey==rhs.encryptionKey
      && full==rhs.full
      && labelLog==rhs.labelLog
      && lastFSeq==rhs.lastFSeq
      && lastModificationLog==rhs.lastModificationLog
      && lastReadLog==rhs.lastReadLog
      && lastWriteLog==rhs.lastWriteLog
      && lbp==rhs.lbp
      && logicalLibraryName==rhs.logicalLibraryName
      && tapePoolName==rhs.tapePoolName
      && vid==rhs.vid;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool cta::common::dataStructures::Tape::operator!=(const Tape &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::Tape &obj) {
  os << "(busy=" << obj.busy
     << " capacityInBytes=" << obj.capacityInBytes
     << " comment=" << obj.comment
     << " creationLog=" << obj.creationLog
     << " dataOnTapeInBytes=" << obj.dataOnTapeInBytes
     << " disabled=" << obj.disabled
     << " encryptionKey=" << obj.encryptionKey
     << " full=" << obj.full
     << " labelLog=" << obj.labelLog
     << " lastFSeq=" << obj.lastFSeq
     << " lastModificationLog=" << obj.lastModificationLog
     << " lastReadLog=" << obj.lastReadLog
     << " lastWriteLog=" << obj.lastWriteLog
     << " lbp=" << obj.lbp
     << " logicalLibraryName=" << obj.logicalLibraryName
     << " tapePoolName=" << obj.tapePoolName
     << " vid=" << obj.vid << ")";
  return os;
}

