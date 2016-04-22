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

#pragma once

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/DRData.hpp"
#include "common/dataStructures/TapeFileLocation.hpp"

namespace cta {
namespace common {
namespace dataStructures {

/*
 * This struct holds all the CTA file metadata (the lastUpdateTime is used for 
 * reconciliation purposes: if a file has not been updated for a long time by 
 * the disk system, it has to be checked against it) 
 */
struct ArchiveFile {

  ArchiveFile();

  bool operator==(const ArchiveFile &rhs) const;

  bool operator!=(const ArchiveFile &rhs) const;

  uint64_t archiveFileID;
  std::string diskFileID;
  std::string diskInstance;
  uint64_t fileSize;
  std::string checksumType;
  std::string checksumValue;
  std::string storageClass;
  cta::common::dataStructures::DRData drData;
  std::map<uint64_t,cta::common::dataStructures::TapeFileLocation> tapeCopies;
  time_t creationTime;
  time_t lastUpdateTime;

}; // struct ArchiveFile

} // namespace dataStructures
} // namespace common
} // namespace cta

std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::ArchiveFile &obj);
