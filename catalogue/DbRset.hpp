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

#include <stdint.h>

namespace cta {
namespace catalogue {

/**
 * Abstract class specificing the interface to the result set of an sql query.
 *
 * Please note that this interface intentionally uses C-strings instead of
 * std::string so that it can be used by code compiled against the CXX11 ABI and
 * by code compiled against a pre-CXX11 ABI.
 */
class DbRset {
public:

  /**
   * Destructor.
   *
   * Please note that this method will delete the memory asscoiated with any
   * C-strings returned by the columnText() method.
   */
  virtual ~DbRset() throw() = 0;

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  virtual const char *getSql() const = 0;

  /**
   * Attempts to get the next row of the result set.
   *
   * Please note that this method will delete the memory associated with any
   * C-strings returned by the columnText() method.
   *
   * @return True if a row has been retrieved else false if there are no more
   * rows in the result set.
   */
  virtual bool next() = 0;

  /**
   * Returns true if the specified column contains a null value.
   *
   * @param colName The name of the column.
   * @return True if the specified column contains a null value.
   */
  virtual bool columnIsNull(const char *const colName) const = 0;

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
  virtual const char *columnText(const char *const colName) const = 0;

  /**
   * Returns the value of the specified column as an integer.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  virtual uint64_t columnUint64(const char *const colName) const = 0;

}; // class DbRset

} // namespace catalogue
} // namespace cta
