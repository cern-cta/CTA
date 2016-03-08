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

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::common::dataStructures::ArchiveFile::operator==(const ArchiveFile &rhs) const {
  return archiveFileID==rhs.archiveFileID
      && checksumType==rhs.checksumType
      && checksumValue==rhs.checksumValue
      && drData==rhs.drData
      && eosFileID==rhs.eosFileID
      && fileSize==rhs.fileSize
      && storageClass==rhs.storageClass
      && tapeCopies==rhs.tapeCopies;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool cta::common::dataStructures::ArchiveFile::operator!=(const ArchiveFile &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::ArchiveFile &obj) {
  os << "(archiveFileID=" << obj.archiveFileID
     << " checksumType=" << obj.checksumType
     << " checksumValue=" << obj.checksumValue
     << " drData=" << obj.drData
     << " eosFileID=" << obj.eosFileID
     << " fileSize=" << obj.fileSize
     << " storageClass=" << obj.storageClass
     << " tapeCopies=" << obj.tapeCopies << ")";
  return os;
}

