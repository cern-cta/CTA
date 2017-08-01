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
    checksumType == rhs.checksumType &&
    checksumValue == rhs.checksumValue &&
    storageClassName == rhs.storageClassName &&
    vid == rhs.vid &&
    fSeq == rhs.fSeq &&
    blockId == rhs.blockId &&
    compressedSize == rhs.compressedSize &&
    copyNb == rhs.copyNb &&
    tapeDrive == rhs.tapeDrive;
}

//------------------------------------------------------------------------------
// operator<
//------------------------------------------------------------------------------
bool TapeFileWritten::operator<(const TapeFileWritten &rhs) const {
  return fSeq < rhs.fSeq;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const TapeFileWritten &obj) {
  os <<
  "{"
  "archiveFileId=" << obj.archiveFileId << ","
  "diskInstance=" << obj.diskInstance << ","
  "diskFileId=" << obj.diskFileId << ","
  "diskFilePath=" << obj.diskFilePath << ","
  "diskFileUser=" << obj.diskFileUser << ","
  "diskFileGroup=" << obj.diskFileGroup << ","
  "diskFileRecoveryBlob=" << obj.diskFileRecoveryBlob << ","
  "size=" << obj.size << ","
  "checksumType=" << obj.checksumType << "checksumValue=" << obj.checksumValue << ","
  "storageClassName=" << obj.storageClassName << ","
  "vid=" << obj.vid << ","
  "fSeq=" << obj.fSeq << ","
  "blockId=" << obj.blockId << ","
  "compressedSize=" << obj.compressedSize << ","
  "copyNb=" << obj.copyNb << ","
  "tapeDrive=" << obj.tapeDrive <<
  "}";
  return os;
}

} // namespace catalogue
} // namespace cta
