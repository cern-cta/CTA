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

#include "catalogue/DbRset.hpp"

#include <memory>
#include <mutex>
#include <occi.h>

namespace cta {
namespace catalogue {

/**
 * Forward declaration to avoid a circular dependency between OcciRset and
 * OcciStmt.
 */
class OcciStmt;

/**
 * A convenience wrapper around an OCCI result set.
 */
class OcciRset: public DbRset {
public:

  /**
   * Constructor.
   *
   * This constructor will throw an exception if the result set is a NULL
   * pointer.
   *
   * @param stmt The OCCI statement.
   * @param rset The OCCI result set.
   */
  OcciRset(OcciStmt &stmt, oracle::occi::ResultSet *const rset);

  /**
   * Destructor.
   *
   * Please note that this method will delete the memory asscoiated with any
   * C-strings returned by the columnText() method.
   */
  virtual ~OcciRset() throw();

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  virtual const char *getSql() const;

  /**
   * Attempts to get the next row of the result set.
   *
   * Please note that this method will delete the memory associated with any
   * C-strings returned by the columnText() method.
   *
   * @return True if a row has been retrieved else false if there are no more
   * rows in the result set.
   */
  virtual bool next();

  /**
   * Returns true if the specified column contains a null value.
   *
   * @param colName The name of the column.
   * @return True if the specified column contains a null value.
   */
  virtual bool columnIsNull(const char *const colName) const;

  /**
   * Returns the value of the specified column as a string.
   *
   * Please note that a C-string is returned instead of an std::string so that
   * this method can be used by code compiled against the CXX11 ABI and by code
   * compiled against a pre-CXX11 ABI.
   *
   * Please note that if the value of the column is NULL within the database
   * then an empty string shall be returned.  Use the columnIsNull() method to
   * determine whether not a column contains a NULL value.
   *
   * @param colName The name of the column.
   * @return The string value of the specified column.  Please note that the
   * returned string should not be deleted.  The string should be copied before
   * the next call to the next() method.  The DbRset class is responsible
   * for freeing the memory.
   */
  virtual const char *columnText(const char *const colName) const;

  /**
   * Returns the value of the specified column as an integer.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  virtual uint64_t columnUint64(const char *const colName) const;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  void close();

  /**
   * Returns the underlying OCCI result set.
   *
   * This method will always return a valid pointer.
   *
   * @return The underlying OCCI result set.
   */
  oracle::occi::ResultSet *get() const;

  /**
   * An alias for the get() method.
   *
   * @return The underlying OCCI result set.
   */
  oracle::occi::ResultSet *operator->() const;

private:

  /**
   * Mutex used to serialize access to this object.
   */
  mutable std::mutex m_mutex;

  /**
   * The OCCI statement.
   */
  OcciStmt &m_stmt;

  /**
   * The OCCI result set.
   */
  oracle::occi::ResultSet *m_rset;

  /**
   * Forward declaration of the nested class ColumnNameIdx that is intentionally
   * hidden in the cpp file of the SqliteRset class.  The class is hidden in
   * order to enable the SqliteRset class to be used by code compiled against
   * the CXX11 ABI and used by code compiled against the pre-CXX11 ABI.
   */
  class ColumnNameToIdx;

  /**
   * Map from column name to column index.
   *
   * Please note that the type of the map is intentionally forward declared in
   * order to avoid std::string being used.  This is to aid with working with
   * pre and post CXX11 ABIs.
   */
  std::unique_ptr<ColumnNameToIdx> m_colNameToIdx;

  /**
   * Forward declaration of the nest class TextColumnCache that is intentionally
   * hidden in the cpp file of the SqliteRset class.  The class is hidden in
   * order to enable the SqliteRset class to be used by code compiled against
   * the CXX11 ABI and used by code compiled against the pre-CXX11 ABI.
   */
  class TextColumnCache;

  /**
   * Map from column name to column text value.  This map is used to cache the
   * results of calling OcciRset::columnText() in the form of C-strings so that
   * the OcciRset class can provide a similar memory management policy for
   * C-strings as the SQLite API.
   */
  std::unique_ptr<TextColumnCache> m_textColumnCache;

  /**
   * Populates the map from column name to column index.
   */
  void populateColNameToIdxMap();

}; // class OcciRset

} // namespace catalogue
} // namespace cta
