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
