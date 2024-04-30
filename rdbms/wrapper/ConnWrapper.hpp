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

#include "rdbms/AutocommitMode.hpp"
#include "rdbms/wrapper/StmtWrapper.hpp"

#include <atomic>
#include <list>
#include <memory>
#include <string>

namespace cta::rdbms::wrapper {

/**
 * Abstract class that specifies the interface to a database connection.
 */
class ConnWrapper {
public:

  /**
   * Destructor.
   */
  virtual ~ConnWrapper() = 0;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  virtual void close() = 0;

  /**
   * Sets the autocommit mode of the connection.
   *
   * @param autocommitMode The autocommit mode of the connection.
   * @throw AutocommitModeNotSupported If the specified autocommit mode is not
   * supported.
   */
  virtual void setAutocommitMode(const AutocommitMode autocommitMode) = 0;

  /**
   * Returns the autocommit mode of the connection.
   *
   * @return The autocommit mode of the connection.
   */
  virtual AutocommitMode getAutocommitMode() const noexcept = 0;

  /**
   * Executes the statement.
   *
   * @param sql The SQL statement.
   */
  virtual void executeNonQuery(const std::string &sql) = 0;

  /**
   * Creates a prepared statement.
   *
   * @param sql The SQL statement.
   * @return The prepared statement.
   */
  virtual std::unique_ptr<StmtWrapper> createStmt(const std::string &sql) = 0;

  /**
   * Commits the current transaction.
   */
  virtual void commit() = 0;

  /**
   * Rolls back the current transaction.
   */
  virtual void rollback() = 0;

  /**
   * Returns the names of all the column and their type as a map for the given 
   * table in the database schema.
   *
   * @param tableName The table name to get the columns.
   * @return The map of types by name of all the columns for the given table in the database schema.
   */
  virtual std::map<std::string, std::string, std::less<>> getColumns(const std::string &tableName) = 0;
  
  /**
   * Returns the names of all the tables in the database schema in alphabetical
   * order.
   *
   * @return The names of all the tables in the database schema in alphabetical
   * order.
   */
  virtual std::list<std::string> getTableNames() = 0;
  
  
  /**
   * Returns the names of all the indices in the database schema in alphabetical
   * order.
   *
   * @return The names of all the indices in the database schema in alphabetical
   * order.
   */
  virtual std::list<std::string> getIndexNames() = 0;

  /**
   * Returns true if this connection is open.
   */
  virtual bool isOpen() const = 0;

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
  virtual std::list<std::string> getSequenceNames()  = 0;
  
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
  virtual std::list<std::string> getTriggerNames()  = 0;
  
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
  virtual std::list<std::string> getParallelTableNames() = 0;
  
  /**
   * Returns the Constraint names of a given table in the database schema
   * 
   * If the underlying database technologies does not support constraints informations
   * this method simply returns an empty list.
   * 
   * @param tableName the table name to get the constraint names from
   * @return the list of the names of the constraints that the given table has.
   */
  virtual std::list<std::string> getConstraintNames(const std::string &tableName) = 0;
  
  /**
   *
   * Returns the stored procedure names of the database
   * 
   * If the underlying database technologies does not support stored procedures informations
   * this method simply returns an empty list.
   * 
   * @return the list of the names of the stored procedures in the database
   */
  virtual std::list<std::string> getStoredProcedureNames() = 0;
  
  /**
   * Returns the synonym names of the database
   * 
   * If the underlying database technologies does not support synonym informations
   * this method simply returns an empty list.
   * 
   * @return the list of the names of the synonyms in the database
   */
  virtual std::list<std::string> getSynonymNames() = 0;

  /**
   * Returns the type names of the database
   * 
   * If the underlying database technologies does not support type informations
   * this method simply returns an empty list.
   * 
   * @return the list of the names of the types in the database
   */
  virtual std::list<std::string> getTypeNames() = 0;
  
}; // class ConnWrapper

} // namespace cta::rdbms::wrapper
