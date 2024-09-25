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
   * Fetching all columns in a cache as std::optional<std::string>
   */
  void fetchAllColumnsToCache() const override;

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
   * Getting index of a column by using the columnIndexCache
   * in order to avoid looking them up every time we fetch columns of
   * each row of a result
   * @param colName
   * @return index of the column
   */
  int getColumnIndex(const std::string& colName) const;

  /**
   * Gets a value from m_columnKeyStringValueCache if it exists
   * @param key
   * @return
   */
  std::optional<std::string> getColumnValueFromCache(const std::string& key) const;

  /**
   * Template method that converts a tring to a required numeric type
   * using our numeric converters from utils, example of a call could be
   * getNumberFromString(colName, colValAsString, utils::toUint8, utils::isValidUInt);
   * and returns the required NumericType, here e.g. uint8_t
   *
   * @tparam NumericType
   * @param colName
   * @param stringValue
   * @param toNumberFunc
   * @param isValidNumber
   * @return
   */
  template<typename NumericType>
  NumericType PostgresRset::getNumberFromString(const std::string& colName,
                                                const std::string& stringValue,
                                                NumericType(*toNumberFunc)(const std::string&),
                                                bool(*isValidNumber)(const std::string&)) const {

    if(!isValidNumber(stringValue)) {
      throw exception::Exception(std::string("Column ") + colName +
                                 std::string(" contains the value ") +
                                 stringValue +
                                 std::string(" which is not a valid since it does not match required numeric type"));
    }
    return toNumberFunc(stringValue);
  }

  /**
   * Clears the async command in process indicator on our conneciton,
   * if we haven't done so already.
   */
  void doClearAsync();
  /**
   * column index cache
   */
  mutable std::unordered_map<std::string, uint64_t> m_columnPQindexCache;
  /**
   * column index cache
   */
  mutable std::unordered_map<std::string, std::optional<std::string>> m_columnKeyStringValueCache;
  /**
   * flag if all columns were fetched already
   */
  bool m_allColumnsFetched = false;
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
