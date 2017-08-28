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

#include "common/optional.hpp"

#include <stdint.h>
#include <string>

namespace cta {
namespace rdbms {

/**
 * Abstract class specifying the interface to an implementation of the result
 * set of an sql query.
 */
class RsetImpl {
public:

  /**
   * Destructor.
   */
  virtual ~RsetImpl() throw() = 0;

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  virtual const std::string &getSql() const = 0;

  /**
   * Attempts to get the next row of the result set.
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
  virtual bool columnIsNull(const std::string &colName) const = 0;

  /**
   * Returns the value of the specified column as a string.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The string value of the specified column.
   */
  virtual optional<std::string> columnOptionalString(const std::string &colName) const = 0;

  /**
   * Returns the value of the specified column as an integer.
   *
   * This method will return a null column value as an optional with no value.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  virtual optional<uint64_t> columnOptionalUint64(const std::string &colName) const = 0;

}; // class RsetImpl

} // namespace rdbms
} // namespace cta
