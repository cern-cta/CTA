/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/exception/Exception.hpp"
#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresConn.hpp"

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresColumn::PostgresColumn(const std::string &colName, const size_t nbRows):
  m_colName(colName),
  m_nbRows(nbRows),
  m_fieldValues(nbRows,std::make_pair(false, std::string())) {
}

//------------------------------------------------------------------------------
// getColName
//------------------------------------------------------------------------------
const std::string &PostgresColumn::getColName() const {
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
void PostgresColumn::setFieldByteA(rdbms::Conn &conn, const size_t index, const std::string &value) {
  auto pgconn_ptr = dynamic_cast<PostgresConn*>(conn.getConnWrapperPtr());
  auto pgconn = pgconn_ptr->get();

  size_t escaped_length;
  auto escapedByteA = PQescapeByteaConn(pgconn, reinterpret_cast<const unsigned char*>(value.c_str()),
    value.length(), &escaped_length);
  std::string escapedStr(reinterpret_cast<const char*>(escapedByteA), escaped_length);
  PQfreemem(escapedByteA);

  copyStrIntoField(index, escapedStr);
}

//------------------------------------------------------------------------------
// getValue
//------------------------------------------------------------------------------
const char *PostgresColumn::getValue(size_t index) const {
  try {
    if(index >= m_nbRows) {
      exception::Exception ex;
      ex.getMessage() << "Field index is outside the available rows:"
        " index=" << index << " m_nbRows=" << m_nbRows;
      throw ex;
    }
    if (m_fieldValues[index].first) {
      return m_fieldValues[index].second.c_str();
    } else {
      return nullptr;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: colName=" + m_colName + ": " +
      ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// copyStrIntoField
//------------------------------------------------------------------------------
void PostgresColumn::copyStrIntoField(const size_t index, const std::string &str) {
  try {
    if(index >= m_nbRows) {
      exception::Exception ex;
      ex.getMessage() << "Field index is outside the available rows:"
        " index=" << index << " m_nbRows=" << m_nbRows;
      throw ex;
    }
    m_fieldValues[index].first = true;
    m_fieldValues[index].second = str;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: colName=" + m_colName + ": " +
      ex.getMessage().str());
  }
}

} // namespace wrapper
} // namespace rdbms
} // namespace cta
