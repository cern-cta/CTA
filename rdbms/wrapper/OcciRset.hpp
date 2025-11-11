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
#include "common/threading/Mutex.hpp"
#include "rdbms/wrapper/ColumnNameToIdx.hpp"
#include "rdbms/wrapper/RsetWrapper.hpp"

#include <memory>
#include <occi.h>

namespace cta::rdbms::wrapper {

/**
 * Forward declaration to avoid a circular dependency between OcciRset and
 * OcciStmt.
 */
class OcciStmt;

/**
 * A convenience wrapper around an OCCI result set.
 */
class OcciRset : public RsetWrapper {
public:
  /**
   * Constructor.
   *
   * This constructor will throw an exception if the result set is a nullptr
   * pointer.
   *
   * @param stmt The OCCI statement.
   * @param rset The OCCI result set.
   */
  OcciRset(OcciStmt& stmt, oracle::occi::ResultSet* const rset);

  /**
   * Destructor.
   */
  ~OcciRset() override;

  /**
   * @class BlobView
   * @brief For interface compatibility only.
   * Not implemented for Oracle DB, since there is no way to gain access to
   * the raw data buffer without making a copy first.
   */
  class BlobView : public IBlobView {
  public:
      BlobView() = delete;

      const unsigned char* data() const override {
          throw std::logic_error("OcciRset::BlobView is not meant to be used");
      }

      std::size_t size() const override {
          throw std::logic_error("OcciRset::BlobView is not meant to be used");
      }

      ~BlobView() override = default;
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
   * if value is Null and throw
   * They are expected to throw directly if the value is null
   * from within their implementatinos without the need
   * to construct additional optionals with possible string copies in between.
   * We currently need these implementations for PG as we did not find a direct
   * culprit why the performance with Optionals is so much slower yet.
   * @param colName
   * @return
   */
  uint8_t columnUint8NoOpt(const std::string& colName) const override { throw cta::exception::Exception("Not implemented"); };

  uint16_t columnUint16NoOpt(const std::string& colName) const override { throw cta::exception::Exception("Not implemented"); };

  uint32_t columnUint32NoOpt(const std::string& colName) const override { throw cta::exception::Exception("Not implemented"); };

  uint64_t columnUint64NoOpt(const std::string& colName) const override { throw cta::exception::Exception("Not implemented"); };

  bool columnExists(const std::string& colName) const override { throw cta::exception::Exception("Not implemented"); };

  std::string columnStringNoOpt(const std::string& colName) const override { throw cta::exception::Exception("Not implemented"); };

  double columnDoubleNoOpt(const std::string& colName) const override { throw cta::exception::Exception("Not implemented"); };

  bool columnBoolNoOpt(const std::string& colName) const override { throw cta::exception::Exception("Not implemented"); };

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
  * Returns the value of the specified column as a view over a binary large object (BLOB).
  *
  * This method extracts and unescapes binary data from the specified column and wraps it
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
   * Mutex used to serialize access to this object.
   */
  mutable threading::Mutex m_mutex;

  /**
   * The OCCI statement.
   */
  OcciStmt& m_stmt;

  /**
   * The OCCI result set.
   */
  oracle::occi::ResultSet* m_rset;

  /**
   * Map from column name to column index.
   */
  ColumnNameToIdx m_colNameToIdx;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  void close();

  /**
   * Populates the map from column name to column index.
   */
  void populateColNameToIdxMap();

};  // class OcciRset

}  // namespace cta::rdbms::wrapper
