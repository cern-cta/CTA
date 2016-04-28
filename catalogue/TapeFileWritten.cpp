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

#include "catalogue/TapeFileWritten.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeFileWritten::TapeFileWritten() :
  archiveFileId(0),
  size(0),
  fSeq(0),
  blockId(0),
  compressedSize(0),
  copyNb(0) {
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapeFileWritten::operator==(const TapeFileWritten &rhs) const {
  return
    archiveFileId == rhs.archiveFileId &&
    diskInstance == rhs.diskInstance &&
    diskFileId == rhs.diskFileId &&
    diskFilePath == rhs.diskFilePath &&
    diskFileUser == rhs.diskFileUser &&
    diskFileGroup == rhs.diskFileGroup &&
    diskFileRecoveryBlob == rhs.diskFileRecoveryBlob &&
    size == rhs.size &&
    checksum == rhs.checksum &&
    storageClassName == rhs.storageClassName &&
    vid == rhs.vid &&
    fSeq == rhs.fSeq &&
    blockId == rhs.blockId &&
    compressedSize == rhs.compressedSize &&
    copyNb == rhs.copyNb;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const TapeFileWritten &event) {
  os <<
  "{"
  "archiveFileId=" << event.archiveFileId <<
  "diskInstance=" << event.diskInstance <<
  "diskFileId=" << event.diskFileId <<
  "diskFilePath=" << event.diskFilePath <<
  "diskFileUser=" << event.diskFileUser <<
  "diskFileGroup=" << event.diskFileGroup <<
  "diskFileRecoveryBlob=" << event.diskFileRecoveryBlob <<
  "size=" << event.size <<
  "checksum=" << event.checksum <<
  "storageClassName=" << event.storageClassName <<
  "vid=" << event.vid <<
  "fSeq=" << event.fSeq <<
  "blockId=" << event.blockId <<
  "compressedSize=" << event.compressedSize <<
  "copyNb=" << event.copyNb <<
  "}";
  return os;
}

} // namespace catalogue
} // namespace cta
