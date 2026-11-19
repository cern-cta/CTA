/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"

#include <common/exception/ChecksumTypeMismatch.hpp>
#include <common/exception/ChecksumValueMismatch.hpp>
#include <map>

namespace cta::checksum {

/*!
 * Enumeration of the supported checksum types
 *
 * We allow all checksum types supported by EOS
 */
enum ChecksumType {
  NONE,      //!< No checksum specified
  ADLER32,   //!< Adler-32 checksum
  CRC32,     //!< CRC-32 checksum
  CRC32C,    //!< CRC-32C checksum
  MD5,       //!< MD5 128-bit hash
  SHA1,      //!< SHA-1 160-bit hash
  CRC64,     //!< CRC-64 checksum
  SHA256,    //!< SHA-256 256-bit hash
  XXHASH64,  //!< xxHash 64-bit hash
  BLAKE3,    //!< BLAKE3 256-bit hash
  HWH64      //!< HighwayHash 64-bit hash
};

/*!
 * String representations of the checksum types
 */
const std::map<ChecksumType, std::string> ChecksumTypeName = {
  {NONE,     "NONE"    },
  {ADLER32,  "ADLER32" },
  {CRC32,    "CRC32"   },
  {CRC32C,   "CRC32C"  },
  {MD5,      "MD5"     },
  {SHA1,     "SHA1"    },
  {CRC64,    "CRC64"   },
  {SHA256,   "SHA256"  },
  {XXHASH64, "XXHASH64"},
  {BLAKE3,   "BLAKE3"  },
  {HWH64,    "HWH64"   }
};

/*!
 * A class to store one or more checksums
 */
class ChecksumBlob {
public:
  /*!
   * Default constructor
   */
  ChecksumBlob() = default;

  /*!
   * Construct and insert one checksum
   */
  ChecksumBlob(ChecksumType type, const std::string& value) { insert(type, value); }

  /*!
   * Construct and insert one 32-bit checksum
   */
  ChecksumBlob(ChecksumType type, uint32_t value) { insert(type, value); }

  /*!
   * Clear all of the checksums in this blob
   */
  void clear() { m_cs.clear(); }

  /*!
   * Insert a new checksum into the blob
   *
   * @param[in] value    Little-endian byte array containing the checksum
   */
  void insert(ChecksumType type, const std::string& value);

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
  void deserialize(const std::string& bytearray);

  /*!
  * Deserialize from a byte array to avoid needing temporary string as input
  */
  void deserialize(const void* data, std::size_t size);

  /*!
   * Deserialize from a byte array. In case of an invalid byte array, use the supplied Adler32 value instead.
   */
  void deserializeOrSetAdler32(const std::string& bytearray, uint32_t adler32);

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
   * Return true if this blob contains the specified checksum type
   *
   * This checks only whether the type exists or not
   */
  bool hasChecksum(ChecksumType type) const { return m_cs.contains(type); }

  /*!
   * Get a const reference to the implementation (for conversion to protobuf)
   */
  const std::map<ChecksumType, std::string>& getMap() const { return m_cs; }

  /*!
   * Return the checksum for the specified key
   */
  std::string at(ChecksumType type) const {
    try {
      return m_cs.at(type);
    } catch (std::out_of_range&) {
      std::stringstream ss;
      ss << ChecksumTypeName.at(type) << " checksum not found. Checksum blob contents: " << *this;
      throw exception::ChecksumTypeMismatch(ss.str());
    }
  }

  /*!
   * Validate one specific checksum type between two blobs
   * Throw an exception if either blob does not contain the requested type
   * or if their values are different. Other checksum types are ignored
   */
  void validateOn(ChecksumType type, const ChecksumBlob& blob) const;

  /*!
   * Validate all checksum types shared by two blobs
   * Throw an exception if the blobs have no checksum type in common or if
   * any shared checksum has a different value. Types present in only one
   * blob are ignored
   */
  void validate(const ChecksumBlob& blob) const;

  /*!
   * Return true if both blobs contain exactly the same checksum types and values
   */
  bool hasSameChecksums(const ChecksumBlob& blob) const { return m_cs == blob.m_cs; }

  /**
   * Adds the first checksum of the list of checksums
   * in the log params container given in parameter
   * @param spc the log param container to put the first checksum in
   */
  void addFirstChecksumToLog(cta::log::ScopedParamContainer& spc) const;

  /*!
   * Convert hexadecimal string to little-endian byte array
   */
  static std::string HexToByteArray(std::string hexString);

  /*!
   * Convert little-endian byte array to hexadecimal string
   */
  static std::string ByteArrayToHex(const std::string& bytearray);

private:
  friend std::ostream& operator<<(std::ostream& os, const ChecksumBlob& csb);

  std::map<ChecksumType, std::string> m_cs;
};

}  // namespace cta::checksum
