/**
 * @project        The CERN Tape Archive (CTA)
 * @copyright	   Copyright © 2021-2022 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
