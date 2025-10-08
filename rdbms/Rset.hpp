/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/InvalidResultSet.hpp"

#include <memory>
#include <optional>
#include <stdint.h>
#include <string>

namespace cta::rdbms {

namespace wrapper {
class RsetWrapper;
class IBlobView;
}

/**
 * A wrapper around an object that iterators over a result set from the
 * execution of a database query.
 *
 * This wrapper permits the user of the rdbms::Stmt::executeQuery() method to
 * use different result set implementations whilst only using a result set type.
 */
class Rset {
public:
  /**
   * Constructor.
   */
  Rset();

  /**
   * Constructor
   *
   * @param impl The object actually implementing this result set
   */
  explicit Rset(std::unique_ptr<wrapper::RsetWrapper> impl);

  /**
   * Destructor.
   */
  ~Rset() noexcept;

  /**
   * Releases the underlying result set.
   *
   * This method is idempotent.
   */
  void reset() noexcept;

  /**
   * Deletion of copy constructor.
   */
  Rset(const Rset&) = delete;

  /**
   * Move constructor.
   *
   * @param other The other object to be moved.
   */
  Rset(Rset&& other) noexcept;

  /**
   * Deletion of copy assignment.
   */
  Rset& operator=(const Rset&) = delete;

  /**
   * Move assignment.
   */
  Rset& operator=(Rset&& rhs);

  // Generic method to handle calls to methods (MethodPtr) of m_impl (ImplPtrT)
  template<auto MethodPtr, typename ImplPtrT>
  decltype(auto) delegateToImpl(ImplPtrT& impl, const std::string& colName) const {
    if (impl == nullptr) {
      throw InvalidResultSet("This result set is invalid");
    }

    try {
      return ((*impl).*MethodPtr)(colName);
    } catch (exception::Exception& ex) {
      ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
      throw;
    }
  }

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  const std::string& getSql() const;

  /**
   * Attempts to get the next row of the result set.
   *
   * @return True if a row has been retrieved else false if there are no more
   * rows in the result set.
   * @throw InvalidResultSet if the result is invalid.
   */
  bool next();

  /**
   * Returns true if the result set does not contain any more rows.
   * @return True if the result set does not contain any more rows.
   */
  bool isEmpty() const;

  /**
   * Returns true if the specified column contains a null value.
   *
   * @param colName The name of the column.
   * @return True if the specified column contains a null value.
   * @throw InvalidResultSet if the result is invalid.
   */
  bool columnIsNull(const std::string& colName) const;

  /**
   * Methods to get values without passing through optionals.
   * The only downside is that these methods throw exception for NULL
   * value inside the specific implementation rather than on the top RSet level
   * and shall be used for columns where NULL is not expected.
   * These methods return the converted strings directly from the Postgres memory buffer
   * and do not have implementations for other DB backends for the moment.
   * They were introduced in order to increase performance since the same ran with the corresponding
   * optional methods made the performance 20 times slower. Many tests were run and code adjustments
   * made in order to minimise string copy in the implementation with optionals,
   * but none has shown performance increase so far.
   *
   * @param colName
   * @return The specified output type based on the converted column string value.
   * @throw InvalidResultSet if the result is invalid. NullDbValue in case null value is present
   *        or any other exception
   *
   */
  uint8_t columnUint8NoOpt(const std::string& colName) const;
  uint16_t columnUint16NoOpt(const std::string& colName) const;
  uint32_t columnUint32NoOpt(const std::string& colName) const;
  uint64_t columnUint64NoOpt(const std::string& colName) const;
  std::string columnStringNoOpt(const std::string& colName) const;
  double columnDoubleNoOpt(const std::string& colName) const;
  bool columnBoolNoOpt(const std::string& colName) const;

  /**
   * Returns the value of the specified column as a binary string (byte array).
   *
   * This method will throw an exception if the value of the specified column
   * is nullptr.
   *
   * @param colName The name of the column.
   * @return The string value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::string columnBlob(const std::string& colName) const;

  std::unique_ptr<wrapper::IBlobView> columnBlobView(const std::string& colName) const;


  /**
   * Returns the value of the specified column as a string.
   *
   * This method will throw an exception if the value of the specified column
   * is nullptr.
   *
   * @param colName The name of the column.
   * @return The string value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::string columnString(const std::string& colName) const;

  /**
   * Returns the value of the specified column as a string.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The string value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::optional<std::string> columnOptionalString(const std::string& colName) const;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will throw an exception if the value of the specified column
   * is nullptr.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  uint8_t columnUint8(const std::string& colName) const;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will throw an exception if the value of the specified column
   * is nullptr.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  uint16_t columnUint16(const std::string& colName) const;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will throw an exception if the value of the specified column
   * is nullptr.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  uint32_t columnUint32(const std::string& colName) const;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will throw an exception if the value of the specified column
   * is nullptr.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  uint64_t columnUint64(const std::string& colName) const;

  /**
   * Returns the value of the specified column as a boolean.
   *
   * Please note that the underlying database column type is expected to be a
   * number where a non-zero value means true and a value of zero means false.
   *
   * This method will throw an exception if the value of the specified column
   * is nullptr.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  bool columnBool(const std::string& colName) const;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::optional<uint8_t> columnOptionalUint8(const std::string& colName) const;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::optional<uint16_t> columnOptionalUint16(const std::string& colName) const;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::optional<uint32_t> columnOptionalUint32(const std::string& colName) const;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::optional<uint64_t> columnOptionalUint64(const std::string& colName) const;

  /**
   * Returns the value of the specified column as a boolean.
   *
   * Please note that the underlying database column type is expected to be a
   * number where a non-zero value means true and a value of zero means false.
   * In case of failure this method falls back to interpreting string 't' or 'true'
   * as true and 'f' or 'false' as false.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::optional<bool> columnOptionalBool(const std::string& colName) const;

  /** Helper function used by columnOptionalBool */
  std::optional<bool> extractBoolFromOptionalUint64(const std::string& colName) const;

  /** Helper function used by columnOptionalBool */
  std::optional<bool> extractBoolFromOptionalString(const std::string& colName) const;
  /**
   * Returns the value of the specified column as a double.
   *
   * This method will throw an exception if the value of the specified column
   * is nullptr.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  double columnDouble(const std::string& colName) const;

  /**
   * Returns the value of the specified column as a double.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::optional<double> columnOptionalDouble(const std::string& colName) const;

private:
  /**
   * The object actually implementing this result set.
   */
  std::unique_ptr<wrapper::RsetWrapper> m_impl;

};  // class Rset

}  // namespace cta::rdbms
