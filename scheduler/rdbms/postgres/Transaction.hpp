/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "rdbms/ConnPool.hpp"

#include <chrono>
#include <thread>

namespace cta::schedulerdb {

class Transaction {
public:
  CTA_GENERATE_EXCEPTION_CLASS(SQLError);
  static constexpr int MAX_TXN_START_RETRIES = 3;
  static constexpr std::chrono::milliseconds BASE_BACKOFF {1000};

  // Constructors
  explicit Transaction(cta::rdbms::ConnPool& connPool, log::LogContext& logContext);

  // Move constructor
  Transaction(Transaction&& other) noexcept;

  // Prohibit move assignment
  Transaction& operator=(Transaction&& other) = delete;

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
  void startWithRetry(cta::rdbms::ConnPool& connPool);

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

  bool isDead() { return !m_begin; }

private:
  bool m_begin = false;
  std::unique_ptr<cta::rdbms::Conn> m_conn;
  log::LogContext& m_lc;
};

}  // namespace cta::schedulerdb
