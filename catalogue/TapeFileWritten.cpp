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

#include "catalogue/TapeFileWritten.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeFileWritten::TapeFileWritten() :
  archiveFileId(0),
  diskFileOwnerUid(0),
  diskFileGid(0),
  size(0),
  blockId(0),
  copyNb(0) {
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapeFileWritten::operator==(const TapeFileWritten &rhs) const {
  return
    TapeItemWritten::operator ==(rhs) &&
    archiveFileId == rhs.archiveFileId &&
    diskInstance == rhs.diskInstance &&
    diskFileId == rhs.diskFileId &&
    diskFileOwnerUid == rhs.diskFileOwnerUid &&
    diskFileGid == rhs.diskFileGid &&
    size == rhs.size &&
    checksumBlob == rhs.checksumBlob &&
    storageClassName == rhs.storageClassName &&
    blockId == rhs.blockId &&
    copyNb == rhs.copyNb &&
    tapeDrive == rhs.tapeDrive;
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
  "diskFileOwnerUid=" << obj.diskFileOwnerUid << ","
  "diskFileGid=" << obj.diskFileGid << ","
  "size=" << obj.size << ","
  "checksumBlob=" << obj.checksumBlob << ","
  "storageClassName=" << obj.storageClassName << ","
  "vid=" << obj.vid << ","
  "fSeq=" << obj.fSeq << ","
  "blockId=" << obj.blockId << ","
  "copyNb=" << obj.copyNb << ","
  "tapeDrive=" << obj.tapeDrive <<
  "}";
  return os;
}

} // namespace catalogue
} // namespace cta
