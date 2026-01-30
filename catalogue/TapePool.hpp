/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"

#include <optional>
#include <ostream>
#include <set>
#include <stdint.h>
#include <string>

namespace cta::catalogue {

/**
 * A tape pool is a logical grouping of tapes, it is used to separate VOs, types
 * of data, and multiple copies of data
 */
struct TapePool {
  /**
   * Constructor that sets all integer member-variables to 0 and booleans to
   * false.
   */
  TapePool() = default;

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
  uint64_t nbPartialTapes = 0;

  /**
   * True if the tapes within this tape pool should be encrypted.
   */
  bool encryption = false;

  /**
   * Tape pool encryption key name.
   */
  std::optional<std::string> encryptionKeyName;

  /**
   * The total number of tapes in the pool.
   */
  uint64_t nbTapes = 0;

  /**
   * The total number of empty tapes in the pool.
   */
  uint64_t nbEmptyTapes = 0;

  /**
   * The total number of disabled tapes in the pool.
   */
  uint64_t nbDisabledTapes = 0;

  /**
   * The total number of full tapes in the pool.
   */
  uint64_t nbFullTapes = 0;

  /**
   * The total number of read-only tapes in the pool.
   */
  uint64_t nbReadOnlyTapes = 0;

  /**
   * The total number of writable tapes in the pool.
   */
  uint64_t nbWritableTapes = 0;

  /**
   * The total capacity of all the tapes in the pool in bytes.
   */
  uint64_t capacityBytes = 0;

  /**
   * The total amount of compressed data written to all the tapes in the pool in
   * bytes.
   */
  uint64_t dataBytes = 0;

  /**
   * The total number of physical files stored in the tape pool.
   *
   * Please note that physical files are only removed when a tape is erased.
   * The deletion of a tape file from the CTA catalogue does NOT decrement the
   * number of physical files on that tape and therefore does NOT decrement the
   * number of physical files stored in the tape pool containing that tape.
   */
  uint64_t nbPhysicalFiles = 0;

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

  /**
   * Set of TapePools used as a supply for this TapePool.
   */
  std::set<std::string> supply_source_set;

  /**
   * Set of TapePools that use this TapePool as their supply.
   */
  std::set<std::string> supply_destination_set;

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

}  // namespace cta::catalogue
