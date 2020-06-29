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

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Tape::Tape():
  lastFSeq(0),
  capacityInBytes(0), 
  dataOnTapeInBytes(0),
  nbMasterFiles(0),
  masterDataInBytes(0),
  full(false),
  disabled(false),
  readOnly(false)
  {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool Tape::operator==(const Tape &rhs) const {
  return vid==rhs.vid
      && lastFSeq==rhs.lastFSeq
      && logicalLibraryName==rhs.logicalLibraryName
      && tapePoolName==rhs.tapePoolName
      && capacityInBytes==rhs.capacityInBytes
      && dataOnTapeInBytes==rhs.dataOnTapeInBytes
      && encryptionKeyName==rhs.encryptionKeyName
      && full==rhs.full
      && disabled==rhs.disabled
      && readOnly==rhs.readOnly
      && creationLog==rhs.creationLog
      && lastModificationLog==rhs.lastModificationLog
      && comment==rhs.comment
      && labelLog==rhs.labelLog
      && lastWriteLog==rhs.lastWriteLog
      && lastReadLog==rhs.lastReadLog;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool Tape::operator!=(const Tape &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const Tape &obj) {
  os << "(vid=" << obj.vid
     << " lastFSeq=" << obj.lastFSeq
     << " logicalLibraryName=" << obj.logicalLibraryName
     << " tapePoolName=" << obj.tapePoolName
     << " capacityInBytes=" << obj.capacityInBytes
     << " dataOnTapeInBytes=" << obj.dataOnTapeInBytes
     << " encryptionKeyName=" << (obj.encryptionKeyName ? obj.encryptionKeyName.value() : "null")
     << " full=" << obj.full
     << " disabled=" << obj.disabled
     << " readOnly=" << obj.readOnly    
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " comment=" << obj.comment
     << " labelLog=" << obj.labelLog
     << " lastWriteLog=" << obj.lastWriteLog
     << " lastReadLog=" << obj.lastReadLog << ")";
  return os;
      }

} // namespace dataStructures
} // namespace common
} // namespace cta
