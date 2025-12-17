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

#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"
#include "rdbms/wrapper/ColumnNameToIdxAndType.hpp"
#include "rdbms/wrapper/RsetWrapper.hpp"

#include <memory>
#include <sqlite3.h>
#include <stdint.h>

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
   * @class BlobView
   * @brief Non-owning read-only view of a SQLite BLOB.
   *
   * This class provides a lightweight view of the BLOB data fetched from a SQLite result set,
   * without copying or owning the memory. The view is only valid until the next call to
   * sqlite3_step() or when the statement is finalized.
   */
  class BlobView : public rdbms::wrapper::IBlobView {
  public:
    BlobView(const unsigned char* data, std::size_t size) : m_data(data), m_size(size) {}

    const unsigned char* data() const final { return m_data; }

    std::size_t size() const final { return m_size; }

  private:
    const unsigned char* m_data;
    std::size_t m_size;
  };

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
   * to construct additional optionals with possible string copies in between.
   * We currently need these implementations for PG as we did not find a direct
   * culprit why the performance with Optionals is so much slower yet.
   * @param colName
   * @return
   */
  uint8_t columnUint8NoOpt(const std::string& colName) const override {
    throw cta::exception::NotImplementedException();
  };

  uint16_t columnUint16NoOpt(const std::string& colName) const override {
    throw cta::exception::NotImplementedException();
  };

  uint32_t columnUint32NoOpt(const std::string& colName) const override {
    throw cta::exception::NotImplementedException();
  };

  uint64_t columnUint64NoOpt(const std::string& colName) const override {
    throw cta::exception::NotImplementedException();
  };

  bool columnExists(const std::string& colName) const override { throw cta::exception::NotImplementedException(); };

  std::string columnStringNoOpt(const std::string& colName) const override {
    throw cta::exception::NotImplementedException();
  };

  double columnDoubleNoOpt(const std::string& colName) const override {
    throw cta::exception::NotImplementedException();
  };

  bool columnBoolNoOpt(const std::string& colName) const override { throw cta::exception::NotImplementedException(); };

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
  * Returns the value of the specified column as a non-owning view over a binary large object (BLOB).
  *
  * This method provides direct access to the binary data stored in the specified column using
  * SQLite's `sqlite3_column_blob()` function. The returned view is a lightweight, non-owning
  * wrapper that exposes a pointer to the raw data and its size.
  *
  * Unlike other implementations (e.g., PostgreSQL), this method does not allocate or manage memory.
  * Instead, the underlying memory is owned and managed by SQLite and remains valid only until the
  * next call to `sqlite3_step()` or until the statement is finalized.
  *
  * The returned view must be used immediately and should not be stored beyond the lifetime
  * of the SQLite statement or result set row.
  *
  * @param colName The name of the column containing the BLOB data.
  * @return A `BlobView`-like object providing access to the raw binary data and its size.
  * @throws Exception If the column name is invalid or data extraction fails.
  */
  std::unique_ptr<rdbms::wrapper::IBlobView> columnBlobView(const std::string& colName) const override;

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
