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

#include "common/checksum/ByteArray.hpp"

#include <ostream>
#include <strings.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ByteArray::ByteArray(): m_size(0), m_bytes(NULL) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ByteArray::ByteArray(const uint32_t arraySize, const uint8_t *const bytes) {
  m_size = arraySize;
  m_bytes = new uint8_t[m_size];

  for(uint32_t i = 0; i < m_size; i++) {
    m_bytes[i] = bytes[i];
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ByteArray::ByteArray(const std::string &bytes) {
  m_size = bytes.size();
  m_bytes = new uint8_t[m_size];

  for(uint32_t i = 0; i < m_size; i++) {
    m_bytes[i] = bytes.at(i);
  }
}

//------------------------------------------------------------------------------
// copy constructor
//------------------------------------------------------------------------------
cta::ByteArray::ByteArray(const uint32_t value) {
  m_size = sizeof value;
  m_bytes = new uint8_t[sizeof value];
  bzero(m_bytes, sizeof m_bytes);

  m_bytes[0] = value       & 0xFF;
  m_bytes[1] = value >>  8 & 0xFF;
  m_bytes[2] = value >> 16 & 0xFF;
  m_bytes[3] = value >> 24 & 0xFF;
}

//------------------------------------------------------------------------------
// copy constructor
//------------------------------------------------------------------------------
cta::ByteArray::ByteArray(const ByteArray &other) {
  m_size = other.m_size;
  m_bytes = new uint8_t[m_size];

  for(uint32_t i = 0; i < m_size; i++) {
    m_bytes[i] = other.m_bytes[i];
  }
} 

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ByteArray::~ByteArray() throw() {
  delete[] m_bytes;
}

//------------------------------------------------------------------------------
// assignment
//------------------------------------------------------------------------------
cta::ByteArray &cta::ByteArray::operator=(const ByteArray &rhs) {
  if(this != &rhs) {
    delete[] m_bytes;

    m_size = rhs.m_size;
    m_bytes = new uint8_t[m_size];

    for(uint32_t i = 0; i < m_size; i++) {
      m_bytes[i] = rhs.m_bytes[i];
    }
  }

  return *this;
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::ByteArray::operator==(const ByteArray &rhs) const {
  if(this == &rhs) {
    return true;
  }

  if(m_size != rhs.m_size) {
    return false;
  }

  for(uint32_t i = 0; i < m_size; i++) {
    if(m_bytes[i] != rhs.m_bytes[i]) {
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool cta::ByteArray::operator!=(const ByteArray &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// getSize
//------------------------------------------------------------------------------
uint32_t cta::ByteArray::getSize() const throw() {
  return m_size;
}

//------------------------------------------------------------------------------
// getBytes
//------------------------------------------------------------------------------
const uint8_t *cta::ByteArray::getBytes() const throw() {
  return m_bytes;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const cta::ByteArray &obj) {
  os << "{";

  const auto size = obj.getSize();
  const auto bytes = obj.getBytes();

  for(uint32_t i = 0; i < size; i++) {
    os << bytes[i];
  }

  os << "}";
  return os;
}
