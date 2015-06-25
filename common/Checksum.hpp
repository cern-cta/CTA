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

#include "common/ByteArray.hpp"

namespace cta {

/**
 * A checksum
 */
class Checksum {
public:

  /**
   * Enumeration of the supported checksum types.
   */
  enum ChecksumType {
    CHECKSUMTYPE_NONE,
    CHECKSUMTYPE_ADLER32};

  /**
   * Thread safe method that returns the string representation of the specified
   * checksum type.
   *
   * @param enumValue The integer value of the type.
   * @return The string representation.
   */
  static const char *checksumTypeToStr(const ChecksumType enumValue) throw();

  /**
   * Constructor.
   *
   * Creates an empty checksum.
   */
  Checksum();

  /**
   * Constructor.
   *
   * @param type The type of the checksum.
   * @param byteArray The checksum as a byte array that can be used to store
   * the checksum in a database alongside its type.
   */
  Checksum(const ChecksumType &type, const ByteArray &byteArray);

  /**
   * Returns the type of the checksum.
   *
   * @return The type of the checksum.
   */
  ChecksumType getType() const throw();

  /**
   * Returns the checksum as a byte array that can be used for storing in a
   * database.
   *
   * @return The checksum as a byte array that can be used for storing in a
   * database.
   */
  const ByteArray &getByteArray() const throw();

  /**
   * Returns a human-readable string representation of the checksum.
   */
  std::string str() const;

private:

  /**
   * The type of the checksum.
   */
  ChecksumType m_type;

  /**
   * The checksum as a byte array that can be used to store the checksum in a
   * database alongside its type.
   */
  ByteArray m_byteArray;

}; // class Checksum

} // namespace cta
