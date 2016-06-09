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

namespace cta {
namespace catalogue {

/**
 * Forward declaration.
 */
class DbConn;

/**
 * A class to automatically rollback a database connection when an instance of
 * the class goes out of scope and has not been explictly cancelled.
 */
class AutoRollback {
public:

  /**
   * Constructor.
   *
   * @param dbConn The database connection or NULL if the no rollback should
   * take place.
   */
  AutoRollback(DbConn *const dbConn);

  /**
   * Prevent copying.
   */
  AutoRollback(const AutoRollback &) = delete;

  /**
   * Destructor.
   */
  ~AutoRollback();

  /**
   * Prevent assignment.
   */
  AutoRollback &operator=(const AutoRollback &) = delete;

  /**
   * Cancel the automatic rollback.
   *
   * This method is idempotent.
   */
  void cancel();

private:

  /**
   * The database connection or NULL if no rollback should take place.
   */
  DbConn *m_dbConn;

}; // class DbLogin

} // namespace catalogue
} // namespace cta
