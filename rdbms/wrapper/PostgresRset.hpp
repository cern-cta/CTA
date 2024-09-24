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

#include "rdbms/wrapper/RsetWrapper.hpp"
#include "rdbms/wrapper/Postgres.hpp"
#include "rdbms/wrapper/PostgresConn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace cta::rdbms::wrapper {

/**
 * The result set of an sql query.
 */
class PostgresRset: public RsetWrapper {
public:

  /**
   * Constructor.
   *
   * @param conn The Conn
   * @param stmt The prepared statement.
   * @param res The result set.
   */
  PostgresRset(PostgresConn &conn, PostgresStmt &stmt, std::unique_ptr<Postgres::ResultItr> resitr);

  /**
   * Destructor.
   */
  ~PostgresRset() override;

   /**
    * Getting index of a column by using the columnIndexCache
    * in order to avoid looking them up every time we fetch columns of
    * each row of a result
    * @param colName
    * @return index of the column
    */
  int getColumnIndex(const std::string& colName) const;

  /**
   * Returns true if the specified column contains a null value.
   *
   * @param colName The name of the column.
   * @return True if the specified column contains a null value.
   */
  bool columnIsNull(const std::string &colName) const override;

  /**
   * Returns the value of the specified column as a binary string (byte array).
   *
   * @param colName The name of the column.
   * @return The string value of the specified column.
   */
  std::string columnBlob(const std::string &colName) const override;

  /**
   * Returns the value of the specified column as a string.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The string value of the specified column.
   */
  std::optional<std::string> columnOptionalString(const std::string &colName) const override;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  std::optional<uint8_t> columnOptionalUint8(const std::string &colName) const override;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  std::optional<uint16_t> columnOptionalUint16(const std::string &colName) const override;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  std::optional<uint32_t> columnOptionalUint32(const std::string &colName) const override;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  std::optional<uint64_t> columnOptionalUint64(const std::string &colName) const override;

  /**
   * Returns the value of the specified column as a double.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  std::optional<double> columnOptionalDouble(const std::string &colName) const override;

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  const std::string &getSql() const override;

  /**
   * Attempts to get the next row of the result set.
   *
   * @return True if a row has been retrieved else false if there are no more
   * rows in the result set.
   */
  bool next() override;

private:

  /**
   * Clears the async command in process indicator on our conneciton,
   * if we haven't done so already.
   */
  void doClearAsync();

  /**
   * column index cache
   */
  mutable std::unordered_map<std::string, uint64_t> m_columnIndexCache;
  /**
   * The SQL connection.
   */
  PostgresConn &m_conn;

  /**
   * The prepared statement.
   */
  PostgresStmt &m_stmt;

  /**
   * The result set iterator
   */
  std::unique_ptr<Postgres::ResultItr> m_resItr;

  /**
   * Indicates we have cleared the async in progress flag of the conneciton.
   * This is to make sure we don't clear it more than once
   */
  bool m_asyncCleared;

  /**
   * Number fetched, used for setting the number of affected rows of the statement.
   */
  uint64_t m_nfetched;

}; // class PostgresRset

} // namespace cta::rdbms::wrapper
