/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/wrapper/Postgres.hpp"
#include "rdbms/wrapper/PostgresConn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"
#include "rdbms/wrapper/RsetWrapper.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace cta::rdbms::wrapper {

/**
 * The result set of an sql query.
 */
class PostgresRset : public RsetWrapper {
public:
  /**
   * Constructor.
   *
   * @param conn The Conn
   * @param stmt The prepared statement.
   * @param res The result set.
   */
  PostgresRset(PostgresConn& conn, PostgresStmt& stmt, std::unique_ptr<Postgres::ResultItr> resitr);

  /**
   * Destructor.
   */
  ~PostgresRset() override;

  /**
   * @class BlobView
   * @brief RAII wrapper for a PostgreSQL unescaped binary blob (bytea).
   *
   * This class provides a safe, read-only view of binary data returned by
   * `PQunescapeBytea`. It manages the lifetime of the allocated memory using
   * a `std::unique_ptr` with a custom deleter (`PQfreemem`), ensuring that
   * the buffer is correctly freed when the object goes out of scope.
   *
   * The blob contents can be accessed using `data()` and `size()` methods.
   *
   * Example usage:
   * @code
   *   BlobView blob = rset.columnBlobView("CHECKSUMBLOB");
   *   const unsigned char* ptr = blob.data();
   *   std::size_t len = blob.size();
   *   // Use ptr and len as needed
   * @endcode
   *
   * Note: This class is non-copyable but movable.
   */
  class BlobView : public rdbms::wrapper::IBlobView {
  public:
    BlobView(unsigned char* ptr, std::size_t size) : m_data(ptr), m_size(size), m_guard(ptr, &PQfreemem) {}

    // Explicitly delete copy constructor and copy assignment
    // (despite already implicitly done for unique pointer)
    BlobView(const BlobView&) = delete;
    BlobView& operator=(const BlobView&) = delete;

    // Move constructor
    BlobView(BlobView&& other) noexcept
        : m_data(other.m_data),
          m_size(other.m_size),
          m_guard(std::move(other.m_guard)) {
      other.m_data = nullptr;
      other.m_size = 0;
    }

    // Move assignment
    BlobView& operator=(BlobView&& other) noexcept {
      if (this != &other) {
        m_data = other.m_data;
        m_size = other.m_size;
        m_guard = std::move(other.m_guard);

        other.m_data = nullptr;
        other.m_size = 0;
      }
      return *this;
    }

    const unsigned char* data() const final { return m_data; }

    std::size_t size() const final { return m_size; }

  private:
    const unsigned char* m_data;
    std::size_t m_size;
    std::unique_ptr<unsigned char, decltype(&PQfreemem)> m_guard;
  };

  /**
   * Returns true if the specified column contains a null value.
   *
   * @param colName The name of the column.
   * @return True if the specified column contains a null value.
   */
  bool columnIsNull(const std::string& colName) const override;

  /**
   * Returns true if the specified column contains a null value.
   *
   * @param ifield The index of the column to check of the column.
   * @return True if the specified column contains a null value.
   */
  bool isPGColumnNull(const int ifield) const;

  /**
   * Returns the value of the specified column as a binary string (byte array).
   *
   * @param colName The name of the column.
   * @return The string value of the specified column.
   */
  std::string columnBlob(const std::string& colName) const override;

  /**
  * Returns the value of the specified column as a view over a binary large object (BLOB).
  *
  * This method extracts and uneicitujjecgntnnlthekjndlgfinekvdbcujgcvfivft
   * escapes binary data from the specified column and wraps it
  * in a `BlobView`, which manages the memory using a unique pointer with a custom deleter.
  * The view provides access to the data via a pointer and size, and the underlying memory
  * remains valid as long as the `BlobView` instance is alive.
  *
  * Note: This method allocates memory using `PQunescapeBytea`, which is automatically
  * freed when the returned `BlobView` is destroyed. This is not a non-owning view;
  * ownership of the decoded buffer is transferred to the `BlobView`.
  *
  * @param colName The name of the column containing the BLOB data.
  * @return A `BlobView` object managing the decoded binary data and its size.
  * @throws NullDbValue If the column value is not null but decoding fails.
  */
  std::unique_ptr<rdbms::wrapper::IBlobView> columnBlobView(const std::string& colName) const override;

  /**
    * Returns the value of the specified column as a binary string (byte array).
    *
    * @param colName The name of the column.
    * @return The string value of the specified column.
    */
  bool columnBoolNoOpt(const std::string& colName) const override;

  /**
   * Returns the value of the specified column as a string.
   *
   * @param colName
   * @return
   */
  std::string columnStringNoOpt(const std::string& colName) const override;

  /**
   * Returns the value of the specified column as an integer.
   *
   * @param colName
   * @return
   */
  uint8_t columnUint8NoOpt(const std::string& colName) const override;

  /**
   * Returns the value of the specified column as an integer.
   *
   * @param colName
   * @return
   */
  uint16_t columnUint16NoOpt(const std::string& colName) const override;

  /**
   * Returns the value of the specified column as an integer.
   *
   * @param colName
   * @return
   */
  uint32_t columnUint32NoOpt(const std::string& colName) const override;

  /**
   * Returns the value of the specified column as an integer.
   *
   * @param colName
   * @return
   */
  uint64_t columnUint64NoOpt(const std::string& colName) const override;

  /**
   * Returns the true if the column exists in the result set.
   *
   * @param colName
   * @return true if the column is in the result set, false otherwise
   */
  bool columnExists(const std::string& colName) const override;

  /**
   * Returns the value of the specified column as an double.
   *
   * @param colName
   * @return
   */
  double columnDoubleNoOpt(const std::string& colName) const override;
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

private:
  /**
   * Getting index of a column by using the columnIndexCache
   * in order to avoid looking them up every time we fetch columns of
   * each row of a result
   * @param colName
   * @return index of the column
   */
  size_t getColumnIndex(const std::string& colName) const;

  /**
   * Template method that converts a string to a required numeric type
   * not used at the moment - might be good replacement in the future
   * @tparam NumericType
   * @param colName
   * @param stringValue
   * @return
   */
  template<typename NumericType>
  NumericType getNumberFromString(const std::string& colName, const std::string& stringValue) const {
    // At compile time ensure the type is numeric (e.g., int, double, uint64_t, etc.)
    static_assert(std::is_arithmetic_v<NumericType>, "NumericType must be a numeric type.");

    // Create a stringstream for converting the string to the target type
    std::istringstream iss(stringValue);
    NumericType result;
    // Try to parse the string as the appropriate numeric type
    iss >> result;
    // If conversion failed or if there are extra characters, throw an error
    if (iss.fail() || !iss.eof()) {
      throw exception::Exception(std::string("Column ") + colName + std::string(" contains the value ") + stringValue
                                 + std::string(" which is not a valid since it does not match required numeric type"));
    }
    return result;
  }

  /**
   * Clears the async command in process indicator on our conneciton,
   * if we haven't done so already.
   */
  void doClearAsync();
  /**
   * column index cache
   */
  mutable std::unordered_map<std::string, int> m_columnPQindexCache;
  /**
   * The SQL connection.
   */
  PostgresConn& m_conn;

  /**
   * The prepared statement.
   */
  PostgresStmt& m_stmt;

  /**
   * The result set iterator
   */
  std::unique_ptr<Postgres::ResultItr> m_resItr;

  /**
   * Indicates we have cleared the async in progress flag of the conneciton.
   * This is to make sure we don't clear it more than once
   */
  bool m_asyncCleared = false;

  /**
   * Number fetched, used for setting the number of affected rows of the statement.
   */
  uint64_t m_nfetched = 0;

};  // class PostgresRset

}  // namespace cta::rdbms::wrapper
