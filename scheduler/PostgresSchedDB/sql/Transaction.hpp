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

namespace cta::postgresscheddb {

class Transaction {
public:
  CTA_GENERATE_EXCEPTION_CLASS(SQLError);

  explicit Transaction(rdbms::ConnPool &connPool);

  /**
   * Prohibit copy construction
   */
  Transaction(Transaction&) = delete;

  /**
   * Prohibit copy assignment
   */
  Transaction &operator=(const Transaction &) = delete;

  /**
   * Get the transactional connection
   *
   * @return txn connection
   */
  rdbms::Conn &conn();

  /**
   * Commit any pending transactions
   * and return the connection
   *
   * @return connection
   */
  rdbms::Conn &getNonTxnConn();
  /**
   * Take out a global advisory transaction lock
   *
   * The lock will be automatically released when the transaction ends (or the connection is terminated).
   *
   * @param lockId  Unique identifier for this lock
   */
  void lockGlobal(uint64_t lockId);

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

private:

  rdbms::Conn m_conn_non_txn;
  rdbms::Conn m_conn;
  bool m_begin = true;
};

} // namespace cta::postgresscheddb
