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

#include <stdint.h>
#include <string>

namespace cta {

/**
 * An array of bytes toegther with its size.
 */
class ByteArray {
public:

  /**
   * Constructor.
   */
  ByteArray();

  /**
   * Constructor.
   *
   * Copies the specified array of bytes into this object.
   *
   * @oaram arraySize The size of the array in bytes.
   * @param bytes A raw array of bytes.
   */
  ByteArray(const uint32_t arraySize, const uint8_t *const bytes);

  /**
   * Constructor.
   *
   * Copies the specified array of bytes into this object.
   *
   * @param bytes A raw array of bytes.
   */
  template<uint32_t arraySize> ByteArray(const uint8_t (&bytes)[arraySize]) {
     m_size = arraySize;
     m_bytes = new uint8_t[arraySize];

     for(uint32_t i = 0; i < arraySize; i++) {
       m_bytes[i] = bytes[i];
     }
  }

  /**
   * Constructor.
   *
   * Copies the specified array of bytes into this object.
   *
   * @param bytes An std::string used to store an array of bytes.
   */
  ByteArray(const std::string &bytes);

  /**
   * Constructor.
   *
   * Copies the specified uint32_t into this object in little endian byte order.
   *
   * @param value The value.
   */
  explicit ByteArray(const uint32_t value);

  /**
   * Copy constructor.
   */
  ByteArray(const ByteArray &other);

  /**
   * Destructor.
   *
   * Deallocates the memory occupied by the array of bytes.
   */
  ~ByteArray() throw();

  /**
   * Assignment operator.
   */
  ByteArray &operator=(const ByteArray &rhs);

  /** 
   * Returns true if the specified right-hand side is equal to this object.
   *
   * @param rhs The object on the right-hand side of the == operator.
   * @return True if the specified right-hand side is equal to this object.
   */
  bool operator==(const ByteArray &rhs) const;

  /**
   * Returns true if the specified right-hand side is not euqal to this object.
   *
   * @param rhs The object on the right-hand side of the != operator.
   * @return True if the specified right-hand side is not equal to this object.
   */
  bool operator!=(const ByteArray &rhs) const;

  /**
   * Returns the size of the array in bytes.
   *
   * @return The size of the array in bytes.
   */
  uint32_t getSize() const throw();

  /**
   * Returns the contents of the array of bytes.
   *
   * This metho shall return NULL if the array of bytes is empty.
   *
   * @return The contents of the array of bytes or NULL if the array is empty.
   */
  const uint8_t *getBytes() const throw();

private:

  /**
   * The size of the array in bytes.
   */
  uint32_t m_size;

  /**
   * The array of bytes allocated on the heap.
   */
  uint8_t *m_bytes;

}; // class ByteArray

} // namespace cta

/**
 * Output stream operator for the cta::ByteArray class.
 */
std::ostream &operator<<(std::ostream &os, const cta::ByteArray &obj);
