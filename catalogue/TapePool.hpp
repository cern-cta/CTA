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

#include <ostream>
#include <stdint.h>
#include <string>

#include "common/dataStructures/EntryLog.hpp"

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
  bool operator==(const TapePool &rhs) const;

  /**
   * Comparison operator.
   *
   * @return True if the names of both tape pools are not equal.
   */
  bool operator!=(const TapePool &rhs) const;

  /**
   * The name of the tape pool.
   */
  std::string name;

  /**
   * The virtual organisation of the tape pool.
   */
  std::string vo;

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
   * The total capacity of all the tapes in the pool in bytes.
   */
  uint64_t capacityBytes;

  /**
   * The total amount of compressed data written to all the tapes in the pool in
   * bytes.
   */
  uint64_t dataBytes;

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

}; // struct TapePool

/**
 * Output stream operator for a TapePool object.
 *
 * This function writes a human readable form of the specified object to the
 * specified output stream.
 *
 * @param os The output stream.
 * @param obj The object.
 */
std::ostream &operator<<(std::ostream &os, const TapePool &obj);

} // namespace catalogue
} // namespace cta
