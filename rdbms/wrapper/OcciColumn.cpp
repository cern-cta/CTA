/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/exception/Exception.hpp"
#include "rdbms/wrapper/OcciColumn.hpp"

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciColumn::OcciColumn(const std::string& colName, const size_t nbRows) :
m_colName(colName),
m_nbRows(nbRows),
m_maxFieldLength(0) {
  m_fieldLengths.reset(new ub2[m_nbRows]);
  ub2* fieldLengths = m_fieldLengths.get();
  if (nullptr == fieldLengths) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed: colName=" << m_colName
                    << ": Failed to allocate array of field"
                       " lengths for database column: nbRows="
                    << nbRows;
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
  try {
    if (nullptr != m_buffer.get()) {
      exception::Exception ex;
      ex.getMessage() << "Failed to set length at index " << index << " to value " << length
                      << ": "
                         "A field length cannot be set after the column buffer has been allocated";
      throw ex;
    }
    if (index > m_nbRows - 1) {
      exception::Exception ex;
      ex.getMessage() << "Failed to set length at index " << index << " to value " << length
                      << ": "
                         "Index is greater than permitted maximum of "
                      << (m_nbRows - 1);
      throw ex;
    }
    m_fieldLengths[index] = length;
    if (length > m_maxFieldLength) {
      m_maxFieldLength = length;
    }
  }
  catch (exception::Exception& ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: colName=" + m_colName + ": " +
                               ex.getMessage().str());
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
  try {
    if (nullptr == m_buffer.get()) {
      const size_t bufSize = m_nbRows * m_maxFieldLength;

      if (0 == bufSize) {
        exception::Exception ex;
        ex.getMessage() << __FUNCTION__
                        << " failed: Failed to allocate buffer for database column:"
                           " The size of the buffer to be allocated is zero which is invalid";
        throw ex;
      }

      m_buffer.reset(new char[bufSize]);
      if (nullptr == m_buffer.get()) {
        exception::Exception ex;
        ex.getMessage() << __FUNCTION__
                        << " failed: Failed to allocate buffer for database column:"
                           " bufSize="
                        << bufSize;
        throw ex;
      }
    }
    return m_buffer.get();
  }
  catch (exception::Exception& ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: colName=" + m_colName + ": " +
                               ex.getMessage().str());
  }
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
  try {
    const size_t strLenIncludingNull = str.length() + 1;
    if (strLenIncludingNull > m_maxFieldLength) {
      exception::Exception ex;
      ex.getMessage() << "String length including the null terminator is greater than the maximum field length:"
                         " strLenIncludingNull="
                      << strLenIncludingNull << " maxFieldLength=" << m_maxFieldLength;
      throw ex;
    }
    char* const buf = getBuffer();
    char* const element = buf + index * m_maxFieldLength;
    strncpy(element, str.c_str(), m_maxFieldLength);
    element[m_maxFieldLength - 1] = '\0';
  }
  catch (exception::Exception& ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: colName=" + m_colName + ": " +
                               ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// setFieldValueToRaw
//------------------------------------------------------------------------------
void OcciColumn::setFieldValueToRaw(size_t index, const std::string& blob) {
  try {
    size_t maxlen = m_maxFieldLength < 2000 ? m_maxFieldLength : 2000;
    if (blob.length() + 2 > maxlen) {
      throw exception::Exception("Blob length=" + std::to_string(blob.length()) + " exceeds maximum field length (" +
                                 std::to_string(maxlen - 2) + ") bytes)");
    }
    uint16_t len = blob.length();
    char* const buf = getBuffer();
    char* const element = buf + index * m_maxFieldLength;
    memcpy(element, &len, 2);
    memcpy(element + 2, blob.c_str(), len);
  }
  catch (exception::Exception& ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: colName=" + m_colName + ": " +
                               ex.getMessage().str());
  }
}

}  // namespace wrapper
}  // namespace rdbms
}  // namespace cta
