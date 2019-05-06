/**
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

#include <ostream>
#include <sstream>
#include <typeinfo>

#include <common/exception/Exception.hpp>
#include <common/checksum/ChecksumTypeMismatch.hpp>
#include <common/checksum/ChecksumValueMismatch.hpp>

namespace cta {

/**
 * A checksum
 */
class Checksum {
public:
  /**
   * Enumeration of the supported checksum types
   */
  enum ChecksumType {
    CHECKSUMTYPE_NONE,
    CHECKSUMTYPE_ADLER32};

  /**
   * Thread safe method that returns the string representation of the specified checksum type
   *
   * @param enumValue The integer value of the type
   * @return The string representation
   */
  static const char *checksumTypeToStr(const ChecksumType enumValue) throw();

  /**
   * Constructor
   *
   * Creates an empty checksum
   */
  Checksum();

  /**
   * Constructor
   *
   * @param type The type of the checksum
   * @param val A numeric value to store in the byte array
   */
  template <typename t>
  Checksum(const ChecksumType &type, t val): m_type(type) { 
    switch (m_type) {
      case CHECKSUMTYPE_ADLER32:
        if (sizeof(t) != 4) {
          std::stringstream err;
          err << "In Checksum::Checksum(type,value): unexpected value size="
                  << sizeof(t) << " expected=4";
          throw cta::exception::Exception(err.str());
        }
        break;
      default:
        throw cta::exception::Exception("In Checksum::Checksum(type,value): unsupported type for any value");
    }
    setNumeric(val);
  }
  
  /**
   * String based constructor
   * 
   * @param url A string describing the type of the checksum
   */
  Checksum(const std::string & url);

  /**
   * Check two checksums are equal, throw an exception if not
   */
  void validate(const Checksum &rhs) const;

  /**
   * Equality operator
   */
  bool operator==(const Checksum &rhs) const;

  /**
   * Inequality operator
   */
  bool operator!=(const Checksum &rhs) const { return !(*this == rhs); }

  /**
   * Returns the type of the checksum
   */
  ChecksumType getType() const throw();

  /**
   * Returns the checksum as a byte array that can be used for storing in a database
   *
   * The bytes of the bytes array are in little-endian order
   *
   * @return The checksum as a byte array that can be used for storing in a database
   */
  const std::string &getByteArray() const throw();

  /**
   * Returns a human-readable string representation of the checksum
   */
  std::string str() const;
  
  template <typename t>
  t getNumeric() const {
    if (m_byteArray.size() != sizeof(t)) {
      std::stringstream err;
      err << "In Checksum::getNumeric<"
              << typeid(t).name() << ">(): wrong size of byte array="
              << m_byteArray.size() << " expected=" << sizeof(t);
      throw cta::exception::Exception(err.str());
    }
    return (*((t*)m_byteArray.data()));
  }
  
  template <typename t>
  void setNumeric(t val) {
    m_byteArray.replace(m_byteArray.begin(), m_byteArray.end(), (char *)&val, sizeof(t));
  }

private:
  ChecksumType m_type;        //!< The type of the checksum.
  std::string m_byteArray;    //!< The checksum as a byte array in little-endian order

}; // class Checksum



/**
 * Writes the specified Checksum object to the specified output stream
 *
 * @param os The output stream
 * @param checksum The checksum
 */
std::ostream &operator<<(std::ostream &os, const Checksum &checksum);

} // namespace cta
