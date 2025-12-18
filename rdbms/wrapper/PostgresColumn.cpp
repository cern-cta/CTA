/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/wrapper/PostgresColumn.hpp"

#include "common/exception/Exception.hpp"
#include "rdbms/wrapper/PostgresConn.hpp"

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresColumn::PostgresColumn(const std::string& colName, const size_t nbRows)
    : m_colName(colName),
      m_nbRows(nbRows),
      m_fieldValues(nbRows, std::make_pair(false, std::string())) {}

//------------------------------------------------------------------------------
// getColName
//------------------------------------------------------------------------------
const std::string& PostgresColumn::getColName() const {
  return m_colName;
}

//------------------------------------------------------------------------------
// getNbRows
//------------------------------------------------------------------------------
size_t PostgresColumn::getNbRows() const {
  return m_nbRows;
}

//------------------------------------------------------------------------------
// setFieldByteA
//------------------------------------------------------------------------------
void PostgresColumn::setFieldByteA(rdbms::Conn& conn, const size_t index, const std::string& value) {
  auto pgconn_ptr = dynamic_cast<PostgresConn*>(conn.getConnWrapperPtr());
  auto pgconn = pgconn_ptr->get();

  size_t escaped_length;
  auto escapedByteA =
    PQescapeByteaConn(pgconn, reinterpret_cast<const unsigned char*>(value.c_str()), value.length(), &escaped_length);
  std::string escapedStr(reinterpret_cast<const char*>(escapedByteA), escaped_length);
  PQfreemem(escapedByteA);

  copyStrIntoField(index, escapedStr);
}

//------------------------------------------------------------------------------
// getValue
//------------------------------------------------------------------------------
const char* PostgresColumn::getValue(size_t index) const {
  if (index >= m_nbRows) {
    exception::Exception ex;
    ex.getMessage() << "Field index is outside the available rows:"
                       " index="
                    << index << " m_nbRows=" << m_nbRows << "colName=" + m_colName;
    throw ex;
  }
  if (m_fieldValues[index].first) {
    return m_fieldValues[index].second.c_str();
  } else {
    return nullptr;
  }
}

//------------------------------------------------------------------------------
// copyStrIntoField
//------------------------------------------------------------------------------
void PostgresColumn::copyStrIntoField(const size_t index, const std::string& str) {
  if (index >= m_nbRows) {
    exception::Exception ex;
    ex.getMessage() << "Field index is outside the available rows:"
                       " index="
                    << index << " m_nbRows=" << m_nbRows << "colName=" + m_colName;
    throw ex;
  }
  m_fieldValues[index].first = true;
  m_fieldValues[index].second = str;
}

}  // namespace cta::rdbms::wrapper
