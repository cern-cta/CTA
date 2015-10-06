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

#include <string>
#include "common/checksum/Checksum.hpp"

namespace cta {
/**
 * A class containing the location and properties of an archive file. 
 */
class ArchiveFile {
public:
  
  /**
   * Constructor.
   */
  ArchiveFile();

  /**
   * Constructor.
   *
   * @param path The location of the file at NS lookup time
   * @param nsHostName The NS host name
   * @param fileId The file ID (to be used in the tape copy header)
   * @param size The file size
   * @param checksum The file checksum
   * @param lastModificationTime The last modification time of the file
   */
  ArchiveFile(const std::string & path, const std::string & nsHostName, uint64_t fileId, 
    uint64_t size, const uint32_t checksum, const time_t lastModificationTime);
  
  /**
   * The location of the file at NS lookup time
   */
  std::string path;
  
  /**
   * The NS host name
   */
  std::string nsHostName;
  
  /**
   * The file ID (to be used in the tape copy header)
   */
  uint64_t fileId;
  
  /**
   * The file size
   */
  uint64_t size;
  
  /**
   * The file checksum
   */
  uint32_t checksum;
  
  /**
   * The last modification time of the file
   */
  time_t lastModificationTime;
};
}
