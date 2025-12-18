/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/wrapper/OcciColumn.hpp"

#include "common/exception/Exception.hpp"

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciColumn::OcciColumn(const std::string& colName, const size_t nbRows) : m_colName(colName), m_nbRows(nbRows) {
  m_fieldLengths.reset(new ub2[m_nbRows]);
  ub2* fieldLengths = m_fieldLengths.get();
  if (nullptr == fieldLengths) {
    exception::Exception ex;
    ex.getMessage() << "Failed to allocate array of field lengths for database column: colName=" << m_colName
                    << " nbRows=" << nbRows;
    throw ex;
  }
  for (size_t i = 0; i < m_nbRows; i++) {
    fieldLengths[i] = 0;
  }
}

//------------------------------------------------------------------------------
// getColName
//------------------------------------------------------------------------------
const std::string& OcciColumn::getColName() const {
  return m_colName;
}

//------------------------------------------------------------------------------
// getNbRows
//------------------------------------------------------------------------------
size_t OcciColumn::getNbRows() const {
  return m_nbRows;
}

//------------------------------------------------------------------------------
// setFieldLen
//------------------------------------------------------------------------------
void OcciColumn::setFieldLen(const size_t index, const ub2 length) {
  if (nullptr != m_buffer.get()) {
    exception::Exception ex;
    ex.getMessage() << "Failed to set length at index " << index << " to value " << length
                    << ": "
                       "A field length cannot be set after the column buffer has been allocated"
                    << " colName=" << m_colName;
    throw ex;
  }
  if (index > m_nbRows - 1) {
    exception::Exception ex;
    ex.getMessage() << "Failed to set length at index " << index << " to value " << length
                    << ": "
                       "Index is greater than permitted maximum of "
                    << (m_nbRows - 1) << " colName=" << m_colName;
    throw ex;
  }
  m_fieldLengths[index] = length;
  if (length > m_maxFieldLength) {
    m_maxFieldLength = length;
  }
}

//------------------------------------------------------------------------------
// getFieldLength
//------------------------------------------------------------------------------
ub2* OcciColumn::getFieldLengths() {
  return m_fieldLengths.get();
}

//------------------------------------------------------------------------------
// getBuffer
//------------------------------------------------------------------------------
char* OcciColumn::getBuffer() {
  if (nullptr == m_buffer.get()) {
    const size_t bufSize = m_nbRows * m_maxFieldLength;

    if (0 == bufSize) {
      exception::Exception ex;
      ex.getMessage() << "Failed to allocate buffer for database column:"
                         " The size of the buffer to be allocated is zero which is invalid"
                      << " colName=" << m_colName;
      throw ex;
    }

    m_buffer.reset(new char[bufSize]);
    if (nullptr == m_buffer.get()) {
      exception::Exception ex;
      ex.getMessage() << "Failed to allocate buffer for database column:"
                         " bufSize="
                      << bufSize << " colName=" << m_colName;
      throw ex;
    }
  }
  return m_buffer.get();
}

//------------------------------------------------------------------------------
// getMaxFieldLength
//------------------------------------------------------------------------------
ub2 OcciColumn::getMaxFieldLength() const {
  return m_maxFieldLength;
}

//------------------------------------------------------------------------------
// copyStrIntoField
//------------------------------------------------------------------------------
void OcciColumn::copyStrIntoField(const size_t index, const std::string& str) {
  char* const buf = getBuffer();
  char* const element = buf + index * m_maxFieldLength;

  element[m_maxFieldLength - 1] = '\0';
  strncpy(element, str.c_str(), m_maxFieldLength);
  if (element[m_maxFieldLength - 1] != '\0') {
    exception::Exception ex;
    ex.getMessage()
      << "String length including the null terminator is greater than the maximum field length: strLenIncludingNull="
      << str.length() + 1 << " maxFieldLength=" << m_maxFieldLength << " colName=" << m_colName;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setFieldValueToRaw
//------------------------------------------------------------------------------
void OcciColumn::setFieldValueToRaw(size_t index, const std::string& blob) {
  {
    size_t maxlen = m_maxFieldLength < 2000 ? m_maxFieldLength : 2000;
    if (blob.length() + 2 > maxlen) {
      throw exception::Exception("Blob length=" + std::to_string(blob.length()) + " exceeds maximum field length ("
                                 + std::to_string(maxlen - 2) + ") bytes" + " colName=" + m_colName);
    }
  }
  uint16_t len = blob.length();
  char* const buf = getBuffer();
  char* const element = buf + index * m_maxFieldLength;
  memcpy(element, &len, 2);
  memcpy(element + 2, blob.c_str(), len);
}

}  // namespace cta::rdbms::wrapper
