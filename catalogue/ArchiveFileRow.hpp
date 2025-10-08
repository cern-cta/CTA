/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/checksum/ChecksumBlob.hpp"

#include <stdint.h>
#include <string>

namespace cta::catalogue {

/**
 * A row in the ArchiveFile table.
 */
struct ArchiveFileRow {

  /**
   * Constructor.
   *
   * Sets the value of all integer member-variables to zero.
   */
  ArchiveFileRow() = default;

  /**
   * Equality operator.
   *
   * @param ths The right hand side of the operator.
   */
  bool operator==(const ArchiveFileRow &rhs) const;

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

  /**
   * Creation time in seconds since the Unix epoch.
   */
  time_t creationTime = 0;

  /**
   * Reconciliation time in seconds since the Unix epoch.
   */
  time_t reconciliationTime = 0;

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

} // namespace cta::catalogue
