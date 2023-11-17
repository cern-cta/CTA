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

namespace cta::rdbms {

/**
 * Forward declaration.
 */
class Conn;

/**
 * A class to automatically rollback a database connection when an instance of
 * the class goes out of scope and has not been explictly cancelled.
 */
class AutoRollback {
public:

  /**
   * Constructor.
   *
   * @param conn The database connection.
   */
  AutoRollback(Conn &conn);

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
   * True if the automatica rollback has been cancelled.
   */
  bool m_cancelled;

  /**
   * The database connection or nullptr if no rollback should take place.
   */
  Conn &m_conn;

}; // class Login

} // namespace cta::rdbms
