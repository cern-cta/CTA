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

#include "common/threading/MutexLocker.hpp"
#include "common/threading/RWLock.hpp"
#include "rdbms/wrapper/ConnWrapper.hpp"

#include <occi.h>

namespace cta {
namespace rdbms {
namespace wrapper {

/**
 * Forward declaraion to avoid a circular dependency beween OcciConn and
 * OcciStmt.
 */
class OcciStmt;

/**
 * A convenience wrapper around a connection to an OCCI database.
 */
class OcciConn: public ConnWrapper {
public:

  /**
   * Constructor.
   *
   * This method will throw an exception if the OCCI connection is a nullptr
   * pointer.
   *
   * @param env The OCCI environment.
   * @param conn The OCCI connection.
   */
  OcciConn(oracle::occi::Environment *env, oracle::occi::Connection *const conn);

  /**
   * Destructor.
   */
  ~OcciConn() override;

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
  std::map<std::string, std::string> getColumns(const std::string &tableName) override;

  /**
   * Returns the names of all the tables in the database schema in alphabetical
   * order.
   *
   * @return The names of all the tables in the database schema in alphabetical
   * order.
   */
  std::list<std::string> getTableNames() override;
  
  /**
   * Returns the names of all the indices the database schema in alphabetical
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

private:

  friend OcciStmt;

  /**
   * Mutex used to serialize access to this object.
   */
  threading::Mutex m_mutex;

  /**
   * The OCCI environment.
   */
  oracle::occi::Environment *m_env;

  /**
   * The OCCI connection.
   */
  oracle::occi::Connection *m_occiConn;

  /**
   * Read-write lock to protect m_autocommitMode.
   */
  mutable threading::RWLock m_autocommitModeRWLock;

  /**
   * The autocommit mode of the connection.
   */
  AutocommitMode m_autocommitMode;

  /**
   * Closes the specified OCCI statement.
   *
   * @param stmt The OCCI statement to be closed.
   */
  void closeStmt(oracle::occi::Statement *const stmt);

}; // class OcciConn

} // namespace wrapper
} // namespace rdbms
} // namespace cta
