/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/threading/Mutex.hpp"
#include "rdbms/ColumnNameToIdx.hpp"
#include "rdbms/RsetImpl.hpp"

#include <memory>
#include <occi.h>

namespace cta {
namespace rdbms {

/**
 * Forward declaration to avoid a circular dependency between OcciRset and
 * OcciStmt.
 */
class OcciStmt;

/**
 * A convenience wrapper around an OCCI result set.
 */
class OcciRsetImpl: public RsetImpl {
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
  OcciRsetImpl(OcciStmt &stmt, oracle::occi::ResultSet *const rset);

  /**
   * Destructor.
   */
  virtual ~OcciRsetImpl() throw() override;

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  virtual const std::string &getSql() const override;

  /**
   * Attempts to get the next row of the result set.
   *
   * @return True if a row has been retrieved else false if there are no more
   * rows in the result set.
   */
  virtual bool next() override;

  /**
   * Returns true if the specified column contains a null value.
   *
   * @param colName The name of the column.
   * @return True if the specified column contains a null value.
   */
  virtual bool columnIsNull(const std::string &colName) const override;

  /**
   * Returns the value of the specified column as a string.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The string value of the specified column.
   */
  virtual optional<std::string> columnOptionalString(const std::string &colName) const override;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  virtual optional<uint64_t> columnOptionalUint64(const std::string &colName) const override;

private:

  /**
   * Mutex used to serialize access to this object.
   */
  mutable threading::Mutex m_mutex;

  /**
   * The OCCI statement.
   */
  OcciStmt &m_stmt;

  /**
   * The OCCI result set.
   */
  oracle::occi::ResultSet *m_rset;

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

}; // class OcciRsetImpl

} // namespace rdbms
} // namespace cta
