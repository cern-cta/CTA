/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>
#include <optional>
#include <stdint.h>
#include <string>

namespace cta::rdbms::wrapper {

/**
 * Abstract class specifying the interface for a read-only binary blob view.
 *
 * Used for exposing database blob data in a backend-agnostic way.
 * Useful to avoid temporary creation of a string
 */
class IBlobView {
public:
  virtual ~IBlobView() = default;

  /** Returns pointer to the raw binary data. */
  virtual const unsigned char* data() const = 0;

  /** Returns size of the blob in bytes. */
  virtual std::size_t size() const = 0;
};

/**
 * Abstract class specifying the interface to an implementation of the result
 * set of an sql query.
 */
class RsetWrapper {
public:
  /**
   * Destructor.
   */
  virtual ~RsetWrapper() = 0;

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  virtual const std::string& getSql() const = 0;

  /**
   * Attempts to get the next row of the result set.
   *
   * @return True if a row has been retrieved else false if there are no more
   * rows in the result set.
   */
  virtual bool next() = 0;

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
  virtual uint8_t columnUint8NoOpt(const std::string& colName) const = 0;
  virtual uint16_t columnUint16NoOpt(const std::string& colName) const = 0;
  virtual uint32_t columnUint32NoOpt(const std::string& colName) const = 0;
  virtual uint64_t columnUint64NoOpt(const std::string& colName) const = 0;
  virtual std::string columnStringNoOpt(const std::string& colName) const = 0;
  virtual bool columnExists(const std::string& colName) const = 0;
  virtual double columnDoubleNoOpt(const std::string& colName) const = 0;
  virtual bool columnBoolNoOpt(const std::string& colName) const = 0;
  // Returns a backend-specific BlobView as a generic IBlobView pointer
  virtual std::unique_ptr<rdbms::wrapper::IBlobView> columnBlobView(const std::string& colName) const = 0;

  /**
   * Returns true if the specified column contains a null value.
   *
   * @param colName The name of the column.
   * @return True if the specified column contains a null value.
   */
  virtual bool columnIsNull(const std::string& colName) const = 0;

  /**
   * Returns the value of the specified column as a binary string (byte array).
   *
   * @param colName The name of the column.
   * @return The string value of the specified column.
   */
  virtual std::string columnBlob(const std::string& colName) const = 0;

  /**
   * Returns the value of the specified column as a string.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The string value of the specified column.
   */
  virtual std::optional<std::string> columnOptionalString(const std::string& colName) const = 0;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  virtual std::optional<uint8_t> columnOptionalUint8(const std::string& colName) const = 0;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  virtual std::optional<uint16_t> columnOptionalUint16(const std::string& colName) const = 0;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  virtual std::optional<uint32_t> columnOptionalUint32(const std::string& colName) const = 0;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  virtual std::optional<uint64_t> columnOptionalUint64(const std::string& colName) const = 0;

  /**
   * Returns the value of the specified column as a double.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  virtual std::optional<double> columnOptionalDouble(const std::string& colName) const = 0;

};  // class RsetWrapper

}  // namespace cta::rdbms::wrapper
