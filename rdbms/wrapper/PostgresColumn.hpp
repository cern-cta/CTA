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

#pragma once

#include <memory>
#include <string.h>
#include <typeinfo>
#include <vector>
#include <libpq-fe.h>
#include <rdbms/Conn.hpp>

namespace cta {
namespace rdbms {
namespace wrapper {

/**
 * A class to help with preparing COPY FROM inserts with postgres.
 */
class PostgresColumn {
public:
  /**
   * Constructor.
   *
   * @param colName The name of the column.
   * @param nbRows The number of rows in the column.
   */
  PostgresColumn(const std::string &colName, const size_t nbRows);

  /**
   * Returns the name of the column.
   *
   * @return The name of the column.
   */
  const std::string &getColName() const;

  /**
   * Returns the number of rows in the column.
   *
   * @return The number of rows in the column.
   */
  size_t getNbRows() const;

  /**
   * Returns a pointer to the column field value at the specified index.
   *
   * @return The pointer to the row value.
   */
  const char *getValue(size_t index) const;

  /**
   * Sets the field at the specified index to the specified value.
   *
   * This method tag dispatches using std::is_integral.
   *
   * @param index The index of the field.
   * @param value The value of the field.
   */
  template<typename T> void setFieldValue(const size_t index, const T &value) {
    setFieldValue(index, value, std::is_integral<T>());
  }

  /**
   * Sets the BYTEA field at the specified index to the value of a byte array
   *
   * @param conn  The connection to the Postgres database
   * @param index The index of the field
   * @param value The value of the field expressed as a byte array
   */
  void setFieldByteA(rdbms::Conn &conn, const size_t index, const std::string &value);

private:

  /**
   * Sets the field at the specified index to the specified value.
   *
   * The third unnamed parameter of this method is used to implement tag
   * dispatching with std::is_integral.
   *
   * @param index The index of the field.
   * @param value The value of the field.
   */
  template<typename T> void setFieldValue(const size_t index, const T &value, std::true_type) {
    copyStrIntoField(index, std::to_string(value));
  }

  /**
   * Sets the field at the specified index to the specified value.
   *
   * The third unnamed parameter of this method is used to implement tag
   * dispatching with std::is_integral.
   *
   * @param index The index of the field.
   * @param value The value of the field.
   */
  template<typename T> void setFieldValue(const size_t index, const T &value, std::false_type) {
    copyStrIntoField(index, value);
  }

  /**
   * Copies the specified string value into the field at the specified index.
   *
   * @param index The index of the field.
   * @param str The string value.
   */
  void copyStrIntoField(const size_t index, const std::string &str);

  /**
   * The name of the column.
   */
  std::string m_colName;

  /**
   * The number of rows in the column.
   */
  size_t m_nbRows;

  /**
   * The array of field values.
   */
  std::vector<std::pair<bool, std::string> > m_fieldValues;

}; // PostgresColumn

} // namespace wrapper
} // namespace rdbms
} // namespace cta
