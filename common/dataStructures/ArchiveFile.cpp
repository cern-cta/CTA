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

#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ArchiveFile::ArchiveFile():
  archiveFileID(0),
  fileSize(0),
  creationTime(0),
  reconciliationTime(0) {
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool ArchiveFile::operator==(const ArchiveFile &rhs) const {
  return archiveFileID == rhs.archiveFileID
      && diskFileId    == rhs.diskFileId
      && diskInstance  == rhs.diskInstance
      && fileSize      == rhs.fileSize
      && checksumType  == rhs.checksumType
      && checksumValue == rhs.checksumValue
      && storageClass  == rhs.storageClass
      && diskFileInfo  == rhs.diskFileInfo
      && tapeFiles    == rhs.tapeFiles;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool ArchiveFile::operator!=(const ArchiveFile &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const ArchiveFile &obj) {
  os <<
    "{"
    "archiveFileID="      << obj.archiveFileID      << ","
    "diskFileID="         << obj.diskFileId         << ","
    "diskInstance="       << obj.diskInstance       << ","
    "fileSize="           << obj.fileSize           << ","
    "checksumType="       << obj.checksumType       << ","
    "checksumValue="      << obj.checksumValue      << ","
    "storageClass="       << obj.storageClass       << ","
    "diskFileInfo="       << obj.diskFileInfo       << ","
    "tapeFiles="          << obj.tapeFiles          << ","
    "creationTime="       << obj.creationTime       << ","
    "reconciliationTime=" << obj.reconciliationTime <<
    "}";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
