/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
   * Constructor
   *
   * @param conn The database connection
   */
  explicit AutoRollback(Conn& conn);

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
