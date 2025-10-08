/**
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/ConnPool.hpp"

namespace cta::schedulerdb {

class Transaction {
public:
  CTA_GENERATE_EXCEPTION_CLASS(SQLError);

  // Constructors
  Transaction(std::unique_ptr<cta::rdbms::Conn> conn, bool ownConnection = false);
  explicit Transaction(cta::rdbms::ConnPool& connPool);

  // Move constructor
  Transaction(Transaction&& other) noexcept;

  // Move assignment operator
  Transaction& operator=(Transaction&& other) noexcept;

  /**
   * Prohibit copy construction
   */
  Transaction(Transaction&) = delete;

  /**
   * Prohibit copy assignment
   */
  Transaction& operator=(const Transaction&) = delete;

  /**
   * Take an advisory transaction lock per tape pool (e.g.)
   * The lock will be automatically released when the
   * transaction ends (or the connection is terminated).
   *
   * @param lockIdString  Unique identifier for this lock
   */
  void takeNamedLock(std::string_view lockIdString);

  /**
   * Take out a exclusive access lock on ARCHIVE_ACTIVE_QUEUE and RETRIEVE_ACTIVE_QUEUE
   *
   * The lock will be automatically released when the transaction ends
   * (or the connection is terminated).
   *
   */
  void lockGlobal();

  /**
   * Commit the transaction
   */
  void commit();

  /**
   * Start new transaction unless it has started already
   */
  void start();

  /**
   * Abort and roll back the transaction
   */
  void abort();

  /**
   * Destructor
   *
   * Aborts the transaction
   */

  ~Transaction() noexcept;

  /**
   * Provides access to the connection
   */
  cta::rdbms::Conn& getConn() const;

  /**
   * Allows to reset the transaction connection to a new one
   *
   * @param connPool
   */
  void resetConn(cta::rdbms::ConnPool& connPool);

private:
  std::unique_ptr<cta::rdbms::Conn> m_conn;
  bool m_ownConnection;
  bool m_begin = false;
};

}  // namespace cta::schedulerdb
