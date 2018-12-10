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

#include "common/threading/Mutex.hpp"
#include "rdbms/wrapper/Conn.hpp"

struct st_mysql;
typedef st_mysql MYSQL;

namespace cta {
namespace rdbms {
namespace wrapper {

class MysqlStmt;

class MysqlConn: public Conn {
public:

  /**
   * The SqliteStmt class can lock the m_mutex member of the SqliteConn class
   * and it can read the pointer to the SQLite connection.
   */
  friend MysqlStmt;


  /**
   * Constructor.
   *
   * @param host The host name.
   * @param user The user name.
   * @param passwd The password.
   * @param db The database name.
   * @param port The port number.
   * 
   */
  MysqlConn(const std::string& host, 
            const std::string& user, 
            const std::string& passwd,
            const std::string& db,
            unsigned int port);

  /**
   * Destructor.
   */
  ~MysqlConn() override;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  void close() override;

  /**
   * Executes the statement.
   *
   * @param sql The SQL statement.
   * @param autocommitMode The autocommit mode of the statement.
   */
  void executeNonQuery(const std::string &sql, const AutocommitMode autocommitMode) override;

  /**
   * Creates a prepared statement.
   *
   * @param sql The SQL statement.
   * @param autocommitMode The autocommit mode of the statement.
   * @return The prepared statement.
   */
  std::unique_ptr<Stmt> createStmt(const std::string &sql) override;

  /**
   * Execute a SQL statement directly, used for non prepared statement.
   *
   * @param sql The SQL statement.
   */
  void execute(const std::string& sql);
  
  /**
   * Commits the current transaction.
   */
  void commit() override;

  /**
   * Rolls back the current transaction.
   */
  void rollback() override;

  /**
   * Returns the names of all the tables in the database schema in alphabetical
   * order.
   *
   * @return The names of all the tables in the database schema in alphabetical
   * order.
   */
  std::list<std::string> getTableNames() override;

  /**
   * Returns true if this connection is open.
   */
  bool isOpen() const override;

  /**
   * Returns the names of all the sequences in the database schema in
   * alphabetical order.
   *
   * If the underlying database technologies does not supported sequences then
   * this method simply returns an empty list.
   *
   * @return The names of all the sequences in the database schema in
   * alphabetical order.
   */
  std::list<std::string> getSequenceNames() override;


private:

  /**
   * Mutex used to serialize access to the database connection.
   */
  threading::Mutex m_mutex;

  /**
   * represent MYSQL connection
   */
  MYSQL* m_mysqlConn;


}; // class MysqlConn


} // namespace wrapper
} // namespace rdbms
} // namespace cta
