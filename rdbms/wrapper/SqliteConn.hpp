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

#include "common/threading/Mutex.hpp"
#include "rdbms/wrapper/ConnWrapper.hpp"

#include <sqlite3.h>

namespace cta::rdbms::wrapper {

/**
 * Forward declaration to avoid a circular dependency between SqliteConn and
 * SqliteStmt.
 */
class SqliteStmt;

/**
 * A convenience wrapper around a connection to an SQLite database.
 */
class SqliteConn: public ConnWrapper {
public:

  /**
   * The SqliteStmt class can lock the m_mutex member of the SqliteConn class
   * and it can read the pointer to the SQLite connection.
   */
  friend SqliteStmt;

  /**
   * Constructor
   *
   * @param filename The filename to be passed to the sqlite3_open() function
   */
  explicit SqliteConn(const std::string& filename);

  /**
   * Destructor.
   */
  ~SqliteConn() override;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  void close() override;

  /**
   * Sets the autocommit mode of the connection.
   *
   * @param autocommitMode The autocommit mode of the connection.
   * @throw AutocommitModeNotSupported If the specified autocommit mode is not
   * supported.
   */
  void setAutocommitMode(const AutocommitMode autocommitMode) override;

  /**
   * Returns the autocommit mode of the connection.
   *
   * @return The autocommit mode of the connection.
   */
  AutocommitMode getAutocommitMode() const noexcept override;

  /**
   * Executes the statement.
   *
   * @param sql The SQL statement.
   */
  void executeNonQuery(const std::string &sql) override;

  /**
   * Creates a prepared statement.
   *
   * @param sql The SQL statement.
   * @return The prepared statement.
   */
  std::unique_ptr<StmtWrapper> createStmt(const std::string &sql) override;

  /**
   * Commits the current transaction.
   */
  void commit() override;

  /**
   * Rolls back the current transaction.
   */
  void rollback() override;

  /**
   * Returns the names of all the column and their type as a map for the given 
   * table in the database schema.
   *
   * @param tableName The table name to get the columns.
   * @return The map of types by name of all the columns for the given table in the database schema.
   */
  std::map<std::string, std::string, std::less<>> getColumns(const std::string &tableName) override;
  
  /**
   * Returns the names of all the tables in the database schema in alphabetical
   * order.
   *
   * @return The names of all the tables in the database schema in alphabetical
   * order.
   */
  std::list<std::string> getTableNames() override;
  
  
  /**
   * Returns the names of all the indices in the database schema in alphabetical
   * order.
   *
   * @return The names of all the indices in the database schema in alphabetical
   * order.
   */
  std::list<std::string> getIndexNames() override;

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
  std::list<std::string> getTriggerNames() override;
  
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
  std::list<std::string> getParallelTableNames() override;
  
  /**
   * Returns the Constraint names of a given table in the database schema
   * @param tableName the table name to get the constraint names from
   * @return the list of the names of the constraints that the given table has.
   */
  std::list<std::string> getConstraintNames(const std::string &tableName) override;
  
  /**
   * 
   * Returns the stored procedure names of the database
   * 
   * If the underlying database technologies does not support stored procedures informations
   * this method simply returns an empty list.
   * 
   * @return the list of the names of the stored procedures in the database
   */
  std::list<std::string> getStoredProcedureNames() override;

  /**
   * Returns the synonym names of the database
   * 
   * If the underlying database technologies does not support synonym informations
   * this method simply returns an empty list.
   * 
   * @return the list of the names of the synonyms in the database
   */
  std::list<std::string> getSynonymNames() override;
  
  /**
   * Returns the type names of the database
   * 
   * If the underlying database technologies does not support type informations
   * this method simply returns an empty list.
   * 
   * @return the list of the names of the types in the database
   */
  std::list<std::string> getTypeNames() override;
  
  /**
   * This is an SqliteConn specific method that prints the database schema to
   * the specified output stream.
   *
   * Please note that this method is intended to be used for debugging.  The
   * output is subjectively not pretty.
   *
   * @param os The output stream.
   */
  void printSchema(std::ostream &os);

private:

  /**
   * Mutex used to serialize access to the database connection.
   */
  threading::Mutex m_mutex;

  /**
   * The database connection.
   */
  sqlite3 *m_sqliteConn;

}; // class SqliteConn

} // namespace cta::rdbms::wrapper
