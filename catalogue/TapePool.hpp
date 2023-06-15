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

#include <optional>
#include <ostream>
#include <stdint.h>
#include <string>

#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"

namespace cta {
namespace catalogue {

/**
 * A tape pool is a logical grouping of tapes, it is used to separate VOs, types
 * of data, and multiple copies of data
 */
struct TapePool {
  /**
   * Constructor that sets all integer member-variables to 0 and booleans to
   * false.
   */
  TapePool();

  /**
   * Comparison operator.
   *
   * @return True if the names of both tape pools are equal.
   */
  bool operator==(const TapePool& rhs) const;

  /**
   * Comparison operator.
   *
   * @return True if the names of both tape pools are not equal.
   */
  bool operator!=(const TapePool& rhs) const;

  /**
   * The name of the tape pool.
   */
  std::string name;

  /**
   * The virtual organisation of the tape pool.
   */
  common::dataStructures::VirtualOrganization vo;

  /**
   * The desired number of tapes that should be empty or partially filled.
   */
  uint64_t nbPartialTapes;

  /**
   * True if the tapes within this tape pool should be encrypted.
   */
  bool encryption;

  /**
   * The total number of tapes in the pool.
   */
  uint64_t nbTapes;

  /**
   * The total number of empty tapes in the pool.
   */
  uint64_t nbEmptyTapes;

  /**
   * The total number of disabled tapes in the pool.
   */
  uint64_t nbDisabledTapes;

  /**
   * The total number of full tapes in the pool.
   */
  uint64_t nbFullTapes;

  /**
   * The total number of read-only tapes in the pool.
   */
  uint64_t nbReadOnlyTapes;

  /**
   * The total number of writable tapes in the pool.
   */
  uint64_t nbWritableTapes;

  /**
   * The total capacity of all the tapes in the pool in bytes.
   */
  uint64_t capacityBytes;

  /**
   * The total amount of compressed data written to all the tapes in the pool in
   * bytes.
   */
  uint64_t dataBytes;

  /**
   * The total number of physical files stored in the tape pool.
   *
   * Please note that physical files are only removed when a tape is erased.
   * The deletion of a tape file from the CTA catalogue does NOT decrement the
   * number of physical files on that tape and therefore does NOT decrement the
   * number of physical files stored in the tape pool containing that tape.
   */
  uint64_t nbPhysicalFiles;

  /**
   * Optional value used by the tape pool supply mechanism.
   */
  std::optional<std::string> supply;

  /**
   * The creation log.
   */
  common::dataStructures::EntryLog creationLog;

  /**
   * The last modification log.
   */
  common::dataStructures::EntryLog lastModificationLog;

  /**
   * The comment.
   */
  std::string comment;

};  // struct TapePool

/**
 * Output stream operator for a TapePool object.
 *
 * This function writes a human readable form of the specified object to the
 * specified output stream.
 *
 * @param os The output stream.
 * @param obj The object.
 */
std::ostream& operator<<(std::ostream& os, const TapePool& obj);

}  // namespace catalogue
}  // namespace cta
