/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "TapeItemWritten.hpp"
#include "common/checksum/ChecksumBlob.hpp"

#include <string>

namespace cta::catalogue {

/**
 * Structure describing the event of having written a file to tape.
 */
struct TapeFileWritten : public TapeItemWritten {
  /**
   * Constructor.
   *
   * Sets the value of all integer member-variables to zero.
   */
  TapeFileWritten() = default;

  /**
   * Equality operator.
   *
   * @param rhs The right hand side of the operator.
   */
  bool operator==(const TapeFileWritten& rhs) const;

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
   * The path of the source disk file within its host disk system.
   */
  std::string diskFilePath;

  /**
   * The user name of the source disk file within its host disk system.
   */
  uint32_t diskFileOwnerUid = 0;

  /**
   * The group name of the source disk file within its host disk system.
   */
  uint32_t diskFileGid = 0;

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
   * The position of the file on tape in the form of its logical block
   * identifier.
   */
  uint64_t blockId = 0;

  /**
   * The copy number of the tape file.
   */
  uint8_t copyNb = 0;

};  // struct TapeFileWritten

/**
 * Output stream operator for an TapeFileWritten object.
 *
 * This function writes a human readable form of the specified object to the
 * specified output stream.
 *
 * @param os The output stream.
 * @param obj The object.
 */
std::ostream& operator<<(std::ostream& os, const TapeFileWritten& obj);

}  // namespace cta::catalogue
