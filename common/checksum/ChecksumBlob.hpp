/*!
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019 CERN
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

#include <map>

#include <common/exception/ChecksumBlobSizeMismatch.hpp>
#include <common/exception/ChecksumTypeMismatch.hpp>
#include <common/exception/ChecksumValueMismatch.hpp>

namespace cta {
namespace checksum {

/*!
 * Enumeration of the supported checksum types
 *
 * We allow all checksum types supported by EOS
 */
enum ChecksumType {
  NONE,       //!< No checksum specified
  ADLER32,    //!< Adler-32 checksum
  CRC32,      //!< CRC-32 checksum
  CRC32C,     //!< CRC-32C checksum
  MD5,        //!< MD5 128-bit hash
  SHA1        //!< SHA-1 160-bit hash
};

/*!
 * String representations of the checksum types
 */
const std::map<ChecksumType, std::string> ChecksumTypeName = {
  { NONE,    "NONE" },
  { ADLER32, "ADLER32" },
  { CRC32,   "CRC32" }, 
  { CRC32C,  "CRC32C" }, 
  { MD5,     "MD5" }, 
  { SHA1,    "SHA1" }
};

/*!
 * A class to store one or more checksums
 */
class ChecksumBlob {
public:
  /*!
   * Default constructor
   */
  ChecksumBlob() {}

  /*!
   * Construct and insert one checksum
   */
  ChecksumBlob(ChecksumType type, const std::string &value) {
    insert(type, value);
  }

  /*!
   * Construct and insert one 32-bit checksum
   */
  ChecksumBlob(ChecksumType type, uint32_t value) {
    insert(type, value);
  }

  /*!
   * Clear all of the checksums in this blob
   */
  void clear() { m_cs.clear(); }

  /*!
   * Insert a new checksum into the blob
   *
   * @param[in] value    Little-endian byte array containing the checksum
   */
  void insert(ChecksumType type, const std::string &value);

  /*!
   * Insert a new 32-bit checksum into the blob
   *
   * Only valid for Adler-32, CRC-32 and CRC-32c checksums. Throws an
   * exception for other checksum types.
   *
   * @param[in] value    32-bit unsigned integer containing the checksum
   */
  void insert(ChecksumType type, uint32_t value);

  /*!
   * Deserialize from a byte array
   */
  void deserialize(const std::string &bytearray);

  /*!
   * Serialise to a byte array
   */
  std::string serialize() const;

  /*!
   * Length of the serialized byte array
   */
  size_t length() const;

  /*!
   * True if there are no checksums in the blob
   */
  bool empty() const { return m_cs.empty(); }

  /*!
   * Returns the number of checksums in the blob
   */
  size_t size() const { return m_cs.size(); }

  /*!
   * Get a const reference to the implementation (for conversion to protobuf)
   */
  const std::map<ChecksumType,std::string> &getMap() const {
    return m_cs;
  }

  /*!
   * Return the checksum for the specified key
   */
  std::string at(ChecksumType type) const {
    return m_cs.at(type);
  }

  /*!
   * Check that a single checksum is in the blob and that it has the value expected, throw an exception if not
   */
  void validate(ChecksumType type, const std::string &value) const;

  /*!
   * Check all the checksums in the blob match, throw an exception if they don't
   */
  void validate(const ChecksumBlob &blob) const;

  /*!
   * Returns true if the checksum is in the blob and that it has the value expected
   */
  bool contains(ChecksumType type, const std::string &value) const {
    try {
      validate(type, value);
    } catch(exception::ChecksumTypeMismatch &ex) {
      return false;
    } catch(exception::ChecksumValueMismatch &ex) {
      return false;
    }
    return true;
  }

  /*!
   * Returns true if all the checksums in the blob match
   */
  bool operator==(const ChecksumBlob &blob) const {
    try {
      validate(blob);
    } catch(exception::ChecksumBlobSizeMismatch &ex) {
      return false;
    } catch(exception::ChecksumTypeMismatch &ex) {
      return false;
    } catch(exception::ChecksumValueMismatch &ex) {
      return false;
    }
    return true;
  }

  /*!
   * Returns false if all the checksums in the blob match
   */
  bool operator!=(const ChecksumBlob &blob) const { return !(*this == blob); }

  /*!
   * Convert hexadecimal string to little-endian byte array
   */
  static std::string HexToByteArray(std::string hexString);

  /*!
   * Convert little-endian byte array to hexadecimal string
   */
  static std::string ByteArrayToHex(const std::string &bytearray);

private:
  friend std::ostream &operator<<(std::ostream &os, const ChecksumBlob &csb);

  std::map<ChecksumType,std::string> m_cs;
};

}} // namespace cta::checksum
