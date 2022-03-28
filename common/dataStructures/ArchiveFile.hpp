/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
  checksum::ChecksumBlob checksumBlob;
  std::string storageClass;
  DiskFileInfo diskFileInfo;
  /**
   * This list represents the non-necessarily-exhaustive set of tape copies 
   * to be listed by the operator. For example, if the listing requested is 
   * for a single tape, the map will contain only one element. 
   */
  class TapeFilesList: public std::list<TapeFile> {
  public:
    using std::list<TapeFile>::list;
    TapeFile & at(uint8_t copyNb);
    const TapeFile & at(uint8_t copyNb) const;
    TapeFilesList::iterator find(uint8_t copyNb);
    TapeFilesList::const_iterator find(uint8_t copyNb) const;
    void removeAllVidsExcept(const std::string &vid);
  };
  TapeFilesList tapeFiles;
  time_t creationTime;
  time_t reconciliationTime;

}; // struct ArchiveFile

std::ostream &operator<<(std::ostream &os, const ArchiveFile &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
