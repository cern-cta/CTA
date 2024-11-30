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

#include "rdbms/wrapper/ColumnNameToIdxAndType.hpp"
#include "rdbms/wrapper/RsetWrapper.hpp"

#include <memory>
#include <stdint.h>
#include <sqlite3.h>

namespace cta::rdbms::wrapper {

/**
 * Forward declaration.
 */
class SqliteStmt;

/**
 * The result set of an sql query.
 */
class SqliteRset : public RsetWrapper {
public:
  /**
   * Constructor
   *
   * @param stmt The prepared statement
   */
  explicit SqliteRset(SqliteStmt& stmt);

  /**
   * Destructor
   */
  ~SqliteRset() override = default;

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  const std::string& getSql() const override;

  /**
   * Attempts to get the next row of the result set.
   *
   * @return True if a row has been retrieved else false if there are no more
   * rows in the result set.
   */
  bool next() override;

  /**
   * Testing new PG methods without passing through optionals
   * and handling the null case within the implementation
   * These methods do not use columnOptional methods to check
   * if value is Null and Throw
   * They are expected to throw directly if the value is null
   * from within their implementatinos ithout the need
   * to construct additional optionals in between !
   * @param colName
   * @return
   */
  uint8_t columnUint8NoOpt(const std::string& colName) const { return 0; };

  uint16_t columnUint16NoOpt(const std::string& colName) const { return 0; };

  uint32_t columnUint32NoOpt(const std::string& colName) const { return 0; };

  uint64_t columnUint64NoOpt(const std::string& colName) const { return 0; };

  std::string columnStringNoOpt(const std::string& colName) const { return std::string(); };

  double columnDoubleNoOpt(const std::string& colName) const { return 0; };

  bool columnBoolNoOpt(const std::string& colName) const { return false; };

  /**
   * Returns true if the specified column contains a null value.
   *
   * @param colName The name of the column.
   * @return True if the specified column contains a null value.
   */
  bool columnIsNull(const std::string& colName) const override;

  /**
   * Returns the value of the specified column as a binary string (byte array).
   *
   * @param colName The name of the column.
   * @return The string value of the specified column.
   */
  std::string columnBlob(const std::string& colName) const override;

  /**
   * Returns the value of the specified column as a string.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The string value of the specified column.
   */
  std::optional<std::string> columnOptionalString(const std::string& colName) const override;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  std::optional<uint8_t> columnOptionalUint8(const std::string& colName) const override;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  std::optional<uint16_t> columnOptionalUint16(const std::string& colName) const override;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  std::optional<uint32_t> columnOptionalUint32(const std::string& colName) const override;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  std::optional<uint64_t> columnOptionalUint64(const std::string& colName) const override;

  /**
   * Returns the value of the specified column as a double.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  std::optional<double> columnOptionalDouble(const std::string& colName) const override;

private:
  /**
   * The prepared statement.
   */
  SqliteStmt& m_stmt;

  /**
   * Map from column name to column index and type.
   */
  ColumnNameToIdxAndType m_colNameToIdxAndType;

  /**
   * Clears and populates the map from column name to column index and type.
   */
  void clearAndPopulateColNameToIdxAndTypeMap();

};  // class SqlLiteRset

}  // namespace cta::rdbms::wrapper
