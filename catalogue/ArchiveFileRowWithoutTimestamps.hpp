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

#include <stdint.h>
#include <string>

namespace cta::catalogue {

/**
 * A row in the ArchiveFile table without the timestamp columns, namely
 * CREATION_TIME and RECONCILIATION_TIME.
 */
struct ArchiveFileRowWithoutTimestamps {

  /**
   * Constructor.
   *
   * Sets the value of all integer member-variables to zero.
   */
  ArchiveFileRowWithoutTimestamps() = default;

  /**
   * Equality operator.
   *
   * @param ths The right hand side of the operator.
   */
  bool operator==(const ArchiveFileRowWithoutTimestamps &rhs) const;

  /**
   * The unique identifier of the file being archived.
   */
  uint64_t archiveFileId = 0;

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
   * The user ID of the owner of the source disk file within its host disk system.
   */
  uint32_t diskFileOwnerUid;

  /**
   * The group ID of the source disk file within its host disk system.
   */
  uint32_t diskFileGid;

  /**
   * The uncompressed size of the tape file in bytes.
   */
  uint64_t size = 0;
  
  /**
   * Set of checksum types and values
   */
  checksum::ChecksumBlob checksumBlob;
  
  /**
   * The name of the file's storage class.
   */
  std::string storageClassName;

}; // struct ArchiveFileRowWithoutTimestamps

/**
 * Output stream operator for an ArchiveFileRowWithoutTimestamps object.
 *
 * This function writes a human readable form of the specified object to the
 * specified output stream.
 *
 * @param os The output stream.
 * @param obj The object.
 */
std::ostream &operator<<(std::ostream &os, const ArchiveFileRowWithoutTimestamps &obj);

} // namespace cta::catalogue
