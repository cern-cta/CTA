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

#include "common/optional.hpp"

#include <ostream>
#include <stdint.h>
#include <string>

namespace cta {
namespace catalogue {

/**
 * A tape that can be written to.
 */
struct TapeForWriting {

  /**
   * Constructor.
   *
   * Sets the value of all integer member-variables to zero.
   *
   * Sets the value of all boolean member-variables to false.
   */
  TapeForWriting();

  /**
   * Equality operator.
   *
   * Two tapes for writing are equal if and only if their volume identifiers
   * (VIDs) are equal.
   *
   * @param ths The right hand side of the operator.
   */
  bool operator==(const TapeForWriting &rhs) const;

  /**
   * The volume identifier of the tape.
   */
  std::string vid;

  /**
   * The media type of the tape.
   */
  std::string mediaType;

  /**
   * The vendor of the tape.
   */
  std::string vendor;
  
  /**
   * The name of the tape pool.
   */
  std::string tapePool;

  /**
   * The file sequence number of the last file successfully written to the tape.
   */
  uint64_t lastFSeq;

  /**
   * The capacity of the tape in bytes.
   */
  uint64_t capacityInBytes;

  /**
   * The total amount of data written to the tape in bytes.
   */
  uint64_t dataOnTapeInBytes;

}; // struct TapeForWriting

/**
 * Output stream operator for an TapeForWriting object.
 *
 * This function writes a human readable form of the specified object to the
 * specified output stream.
 *
 * @param os The output stream.
 * @param obj The object.
 */
std::ostream &operator<<(std::ostream &os, const TapeForWriting &obj);

} // namespace catalogue
} // namespace cta
