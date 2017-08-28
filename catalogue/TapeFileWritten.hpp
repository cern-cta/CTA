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
 * Structure describing the event of having written a file to tape.
 */
struct TapeFileWritten {

  /**
   * Constructor.
   *
   * Sets the value of all integer member-variables to zero.
   */
  TapeFileWritten();

  /**
   * Equality operator.
   *
   * @param rhs The right hand side of the operator.
   */
  bool operator==(const TapeFileWritten &rhs) const;

  /**
   * Less than operator.
   *
   * TapeFileWritten events are ordered by their tape file sequence number.
   *
   * TapeFileWritten events are written to the catalogue database in batches in
   * order to improve performance by reducing the number of network round trips
   * to the database.  Each batch is ordered by tape file sequence number so
   * that the CTA catalogue code can easily assert that files that are written
   * to tape are reported correctly.
   *
   * @param rhs The right hand side of the operator.
   */
  bool operator<(const TapeFileWritten &rhs) const;

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
   * Opaque blob containing the metadata of the source disk file within its host
   * disk system.  This blob can be used in a disaster recovery scenario to
   * contribute to reconstructing the namespace of the host disk system.
   */
  std::string diskFileRecoveryBlob;

  /**
   * The uncompressed size of the tape file in bytes.
   */
  uint64_t size;

  /**
   * Checksum type for the tape file contents.
   */
  std::string checksumType;

  /**
   * Checksum value for the tape file contents.
   */
  std::string checksumValue;
  
  /**
   * The name of the file's storage class.
   */
  std::string storageClassName;

  /**
   * The volume identifier of the tape on which the file has been written.
   */
  std::string vid;

  /**
   * The position of the file on tape in the form of its file sequence number.
   */
  uint64_t fSeq;

  /**
   * The position of the file on tape in the form of its logical block
   * identifier.
   */
  uint64_t blockId;

  /**
   * The compressed size of the tape file in bytes.  In other words the actual
   * number of bytes it occupies on tape.
   */
  uint64_t compressedSize;

  /**
   * The copy number of the tape file.
   */
  uint64_t copyNb;

  /**
   * The name of the tape drive that wrote the file.
   */
  std::string tapeDrive;

}; // struct TapeFileWritten

/**
 * Output stream operator for an TapeFileWritten object.
 *
 * This function writes a human readable form of the specified object to the
 * specified output stream.
 *
 * @param os The output stream.
 * @param obj The object.
 */
std::ostream &operator<<(std::ostream &os, const TapeFileWritten &obj);

} // namespace catalogue
} // namespace cta
