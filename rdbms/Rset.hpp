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

#include "rdbms/InvalidResultSet.hpp"

#include <memory>
#include <optional>
#include <stdint.h>
#include <string>

namespace cta::rdbms {

namespace wrapper {
  class RsetWrapper;
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
  Rset(const Rset &) = delete;

  /**
   * Move constructor.
   *
   * @param other The other object to be moved.
   */
  Rset(Rset &&other);

  /**
   * Deletion of copy assignment.
   */
  Rset &operator=(const Rset &) = delete;

  /**
   * Move assignment.
   */
  Rset &operator=(Rset &&rhs);

  /**
   * Fetching all columns in a cache as std::optional<std::string>
   * currently available only for PostgreSQL implementation
   */
  void fetchAllColumnsToCache() const;

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  const std::string &getSql() const;

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
  bool columnIsNull(const std::string &colName) const;

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
  std::string columnBlob(const std::string &colName) const;

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
  std::string columnString(const std::string &colName) const;

  /**
   * Returns the value of the specified column as a string.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The string value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::optional<std::string> columnOptionalString(const std::string &colName) const;

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
  uint8_t columnUint8(const std::string &colName) const;

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
  uint16_t columnUint16(const std::string &colName) const;

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
  uint32_t columnUint32(const std::string &colName) const;

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
  uint64_t columnUint64(const std::string &colName) const;

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
  bool columnBool(const std::string &colName) const;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::optional<uint8_t> columnOptionalUint8(const std::string &colName) const;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::optional<uint16_t> columnOptionalUint16(const std::string &colName) const;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::optional<uint32_t> columnOptionalUint32(const std::string &colName) const;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::optional<uint64_t> columnOptionalUint64(const std::string &colName) const;

  /**
   * Returns the value of the specified column as a boolean.
   *
   * Please note that the underlying database column type is expected to be a
   * number where a non-zero value means true and a value of zero means false.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::optional<bool> columnOptionalBool(const std::string &colName) const;

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
  double columnDouble(const std::string &colName) const;

  /**
   * Returns the value of the specified column as a double.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   * @throw InvalidResultSet if the result is invalid.
   */
  std::optional<double> columnOptionalDouble(const std::string &colName) const;

private:

  /**
   * The object actually implementing this result set.
   */
  std::unique_ptr<wrapper::RsetWrapper> m_impl;

}; // class Rset

} // namespace cta::rdbms
