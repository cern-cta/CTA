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

#include "common/exception/Exception.hpp"
#include "rdbms/AutocommitMode.hpp"
#include "rdbms/ConnAndStmts.hpp"
#include "rdbms/Stmt.hpp"

#include <list>
#include <memory>

namespace cta {
namespace rdbms {

class ConnPool;

/**
 * A smart database connection that will automatically return the underlying
 * database connection to its parent connection pool when it goes out of scope.
 */
class Conn {
public:
  /**
   * Constructor.
   */
  Conn();

  /**
   * Constructor.
   *
   * @param connAndStmts The database connection and its pool of prepared
   * statements.
   * @param pool The database connection pool to which the connection
   * should be returned.
   */
  Conn(std::unique_ptr<ConnAndStmts> connAndStmts, ConnPool* const pool);

  /**
   * Deletion of the copy constructor.
   */
  Conn(Conn&) = delete;

  /**
   * Move constructor.
   *
   * @param other The other object.
   */
  Conn(Conn&& other);

  /**
   * Destructor.
   *
   * Returns the database connection back to its pool.
   */
  ~Conn() noexcept;

  /**
   * Returns the database connection back to its pool.
   *
   * This method is idempotent.
   */
  void reset() noexcept;

  /**
   * Deletion of the copy assignment operator.
   */
  Conn& operator=(const Conn&) = delete;

  /**
   * Move assignment operator.
   *
   * @param rhs The object on the right-hand side of the operator.
   * @return This object.
   */
  Conn& operator=(Conn&& rhs);

  /**
   * Thrown when a requested autocommit mode is not supported.
   */
  struct AutocommitModeNotSupported : public exception::Exception {
    AutocommitModeNotSupported(const std::string& context = "", const bool embedBacktrace = true) :
    Exception(context, embedBacktrace) {}
  };

  /**
   * Sets the autocommit mode of the connection.
   *
   * @param autocommitMode The autocommit mode of the connection.
   * @throw AutocommitModeNotSupported If the specified autocommit mode is not
   * supported.
   */
  void setAutocommitMode(const AutocommitMode autocommitMode);

  /**
   * Returns the autocommit mode of the connection.
   *
   * @return The autocommit mode of the connection.
   */
  AutocommitMode getAutocommitMode() const;

  /**
   * Creates a prepared statement.
   *
   * @param sql The SQL statement.
   * @return The prepared statement.
   */
  Stmt createStmt(const std::string& sql);

  /**
   * Executes the statement.
   *
   * @param sql The SQL statement.
   */
  void executeNonQuery(const std::string& sql);

  /**
   * Commits the current transaction.
   */
  void commit();

  /**
   * Rolls back the current transaction.
   */
  void rollback();

  /**
   * Returns the names of all the column and their type as a map for the given 
   * table in the database schema.
   *
   * @param tableName The table name to get the columns.
   * @return The map of types by name of all the columns for the given table in the database schema.
   */
  std::map<std::string, std::string> getColumns(const std::string& tableName) const;

  /**
   * Returns the names of all the tables in the database schema in alphabetical
   * order.
   *
   * @return The names of all the tables in the database schema in alphabetical
   * order.
   */
  std::list<std::string> getTableNames() const;

  /**
   * Returns the names of all the indices  in the database schema in alphabetical
   * order.
   *
   * @return The names of all the indices in the database schema in alphabetical
   * order.
   */
  std::list<std::string> getIndexNames() const;

  /**
   * Closes the underlying cached database statements and their connection.
   *
   * This method should only be used in extreme cases where the closure of thei
   * underlying database connection needs to be forced.
   */
  void closeUnderlyingStmtsAndConn();

  /**
   * Returns true if this connection is open.
   */
  bool isOpen() const;

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
  std::list<std::string> getSequenceNames();

  /**
   * Returns the names of all the triggers in the database schema in
   * alphabetical order.
   *
   * If the underlying database technologies does not supported triggers then
   * this method simply returns an empty list.
   *
   * @return The names of all the triggers in the database schema in
   * alphabetical order.
   */
  std::list<std::string> getTriggerNames();

  /**
   * Returns the names of all the tables that have been set as PARALLEL
   * in alphabetical order.
   * 
   * If the underlying database technologies does not support PARALLEL
   * them this method simply returns an empty list.
   * 
   * @return the names of all the tables that have been set as PARALLEL
   * in alphabetical order. 
   */
  std::list<std::string> getParallelTableNames();

  /**
   * Returns the Constraint names of a given table in the database schema
   * 
   * If the underlying database technologies does not support constraints informations
   * this method simply returns an empty list.
   * 
   * @param tableName the table name to get the constraint names from
   * @return the list of the names of the constraints that the given table has.
   */
  std::list<std::string> getConstraintNames(const std::string& tableName);

  /**
   * Returns the stored procedure names of the database
   * 
   * If the underlying database technologies does not support stored procedures informations
   * this method simply returns an empty list.
   * 
   * @return the list of the names of the stored procedures in the database
   */
  std::list<std::string> getStoredProcedureNames();

  /**
   * Returns the synonym names of the database
   * 
   * If the underlying database technologies does not support synonym informations
   * this method simply returns an empty list.
   * 
   * @return the list of the names of the synonyms in the database
   */
  std::list<std::string> getSynonymNames();

  /**
   * Returns the type names of the database
   * 
   * If the underlying database technologies does not support type informations
   * this method simply returns an empty list.
   * 
   * @return the list of the names of the types in the database
   */
  std::list<std::string> getTypeNames();

  /**
   * Get a pointer to the connection wrapper implementation
   *
   * Required for Postgres PQescapeByteaConn()
   */
  wrapper::ConnWrapper* getConnWrapperPtr() { return m_connAndStmts->conn.get(); }

private:
  /**
   * The database connection and its pool of prepared statements.
   */
  std::unique_ptr<ConnAndStmts> m_connAndStmts;

  /**
   * The database connection pool to which the m_conn should be returned.
   */
  ConnPool* m_pool;

};  // class Conn

}  // namespace rdbms
}  // namespace cta
