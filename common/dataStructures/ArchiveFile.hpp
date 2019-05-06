/*!
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019 CERN
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

#pragma once

#include <list>
#include <map>
#include <string>

#include "common/dataStructures/DiskFileInfo.hpp"
#include "common/dataStructures/TapeFile.hpp"
#include "common/checksum/ChecksumBlob.hpp"

namespace cta {
namespace common {
namespace dataStructures {

/**
 * This struct holds all the CTA file metadata 
 */
struct ArchiveFile {

  ArchiveFile();

  /**
   * Equality operator that does NOT compare the creationTime and
   * reconciliationTime member-variables.
   *
   * @param rhs The operand on the right-hand side of the operator.
   * @return True if the compared objects are equal (ignoring the creationTime
   * and reconciliationTime member-variables).
   */
  bool operator==(const ArchiveFile &rhs) const;

  bool operator!=(const ArchiveFile &rhs) const;

  uint64_t archiveFileID;
  std::string diskFileId;
  std::string diskInstance;
  uint64_t fileSize;
  ChecksumBlob checksumBlob;
  std::string storageClass;
  DiskFileInfo diskFileInfo;
  /**
   * This map represents the non-necessarily-exhaustive set of tape copies 
   * to be listed by the operator. For example, if the listing requested is 
   * for a single tape, the map will contain only one element. 
   */
  class TapeFilesList: public std::list<TapeFile> {
  public:
    using std::list<TapeFile>::list;
    TapeFile & at(uint32_t copyNb);
    const TapeFile & at(uint32_t copyNb) const;
    TapeFilesList::iterator find(uint32_t copyNb);
    TapeFilesList::const_iterator find(uint32_t copyNb) const;
  };
  TapeFilesList tapeFiles;
  time_t creationTime;
  time_t reconciliationTime;

}; // struct ArchiveFile

std::ostream &operator<<(std::ostream &os, const ArchiveFile &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
