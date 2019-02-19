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

#include "common/checksum/Checksum.hpp"

#include <stdint.h>
#include <string>

namespace cta {
namespace catalogue {

/**
 * A row in the ArchiveFile table.
 */
struct ArchiveFileRow {

  /**
   * Constructor.
   *
   * Sets the value of all integer member-variables to zero.
   */
  ArchiveFileRow();

  /**
   * Equality operator.
   *
   * @param ths The right hand side of the operator.
   */
  bool operator==(const ArchiveFileRow &rhs) const;

  /**
   * The unique identifier of the file being archived.
   */
  uint64_t archiveFileId;

  /**
   * The instance name of the source disk system.
   */
  std::string diskInstance;

  /**
   * The identifier of the source disk file which is unique within it's host
   * disk system.  Two files from different disk systems may have the same
   * identifier.  The combination of diskInstance and diskFileId must be
   * globally unique, in other words unique within the CTA catalogue.
   */
  std::string diskFileId;

  /**
   * The path of the source disk file within its host disk system.
   */
  std::string diskFilePath;

  /**
   * The user name of the source disk file within its host disk system.
   */
  std::string diskFileUser;

  /**
   * The group name of the source disk file within its host disk system.
   */
  std::string diskFileGroup;

  /**
   * The uncompressed size of the tape file in bytes.
   */
  uint64_t size;
  
  /**
   * Checksum type of the tape file content
   */
  std::string checksumType;
  
  /**
   * Checksum value of the file type content
   */
  std::string checksumValue;

  /**
   * The name of the file's storage class.
   */
  std::string storageClassName;

}; // struct ArchiveFileRow

/**
 * Output stream operator for an ArchiveFileRow object.
 *
 * This function writes a human readable form of the specified object to the
 * specified output stream.
 *
 * @param os The output stream.
 * @param obj The object.
 */
std::ostream &operator<<(std::ostream &os, const ArchiveFileRow &obj);

} // namespace catalogue
} // namespace cta
