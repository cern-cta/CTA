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

#include "common/checksum/ChecksumBlob.hpp"
#include "TapeItemWritten.hpp"

#include <string>

namespace cta::catalogue {

/**
 * Structure describing the event of having written a file to tape.
 */
struct TapeFileWritten: public TapeItemWritten {

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
  uint32_t diskFileOwnerUid;

  /**
   * The group name of the source disk file within its host disk system.
   */
  uint32_t diskFileGid;

  /**
   * The uncompressed size of the tape file in bytes.
   */
  uint64_t size;

  /**
   * Set of checksum types and values
   */
  checksum::ChecksumBlob checksumBlob;

  /**
   * The name of the file's storage class.
   */
  std::string storageClassName;

  /**
   * The position of the file on tape in the form of its logical block
   * identifier.
   */
  uint64_t blockId;

  /**
   * The copy number of the tape file.
   */
  uint8_t copyNb;

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

} // namespace cta::catalogue
