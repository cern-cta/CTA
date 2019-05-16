/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
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

#include "common/optional.hpp"
#include "common/threading/RWLock.hpp"
#include "rdbms/wrapper/StmtWrapper.hpp"
#include "rdbms/wrapper/Postgres.hpp"
#include "rdbms/wrapper/PostgresConn.hpp"
#include "rdbms/wrapper/PostgresColumn.hpp"

#include <vector>
#include <string>
#include <memory>
#include <cstdint>

namespace cta {
namespace rdbms {
namespace wrapper {

class PostgresRset;

/**
 * A convenience wrapper around a postgres prepared statement.
 */
class PostgresStmt: public StmtWrapper {
public:

  /**
   * The PostgresRset class needs to use the lock in this class and a private method.
   */
  friend PostgresRset;

  /**
   * Constructor.
   *
   * This method is called by the PostgresConn::createStmt() method and assumes
   * that a rd lock has already been taken on PostgresConn lock.
   *
   * @param conn The database connection.
   * @param sql The SQL statement.
   */
  PostgresStmt(PostgresConn &conn, const std::string &sql);

  /**
   * Destructor.
   */
  ~PostgresStmt() override;

  /** 
   * Binds an SQL parameter of type optional-string.
   *
   * Please note that this method will throw an exception if the optional string
   * parameter has the empty string as its value.  An optional string parameter
   * should either have a non-empty string value or no value at all.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */ 
  void bindOptionalString(const std::string &paramName, const optional<std::string> &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindOptionalUint64(const std::string &paramName, const optional<uint64_t> &paramValue) override;

  /** 
   * Binds an SQL parameter of type string.
   *
   * Please note that this method will throw an exception if the string
   * parameter is empty.  If a null value is to be bound then the
   * bindOptionalString() method should be used.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */ 
  void bindString(const std::string &paramName, const std::string &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint64(const std::string &paramName, const uint64_t paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindDouble(const std::string &paramName, const double paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindOptionalDouble(const std::string &paramName, const optional<double> &paramValue) override;

  /**
   * Clears the prepared statement so that it is ready to be reused.
   */
  void clear() override;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  void close() override;

  /**
   * Runs the statements command, which could be a COPY FROM command,
   * to do a batch insert rows using the columns set with calls to setColumn
   *
   * @param rows The number of rows
   */
  void executeCopyInsert(const size_t rows);

  /**
   * Executes the statement.
   */
  void executeNonQuery() override;

  /**
   * Executes the statement and returns the result set.
   *
   * @return The result set.
   */
  std::unique_ptr<RsetWrapper> executeQuery() override;

  /**
   * Returns the number of rows affected by the last execution of this
   * statement.
   *
   * @return The number of affected rows.
   */
  uint64_t getNbAffectedRows() const override;

  /**
   * Sets the specified column data for a batch-based database access.
   * Does not take ownership of the PostgresColumn object, the column
   * object must remain valid until after executeCopyInsert has returned.
   *
   * @param col The column data.
   */
  void setColumn(PostgresColumn &col);

private:

  /**
   * Similar to the public clear() method, but without locking.
   */
  void clearAssumeLocked();

  /**
   * Similar to the public close() method, but without locking.
   */
  void closeAssumeLocked();

  /**
   * Close both statement and then connection. Used if the conneciton is broken.
   */
  void closeBoth();

  /**
   * tranforms the sql to format needed for postgres's prepared statements
   * e.g. changes named bind paramters to $1, $2, ... format and counts the bind variables.
   *
   * @param common_sql The sql in the common format, i.e. :VAR_NAME, :VAR2_NAME format.
   * @param pg_sql The transformed SQL, i.e. with $1, $2 format.
   * @param nParams Output the counted number of bind variables.
   */
  void CountAndReformatSqlBinds(const std::string &common_sql, std::string &pg_sql, int &nParams) const;

  /**
   * Throws a LostDatabaseConnection exception if it can determine the
   * the connection has been lost.
   */
  void doConnectionCheck();

  /**
   * Copies the data from the supplied PostgresColumns and sends to PQputCopyData
   * in the appropriate format.
   *
   * @oaran rows The number of rows to be sent to the DB connection
   */
  void doCopyData(const size_t rows);

  /**
   * Starts async execution of prepared tatement on the postgres connection.
   */
  void doPQsendPrepared();

  /**
   * Sends the statement's SQL to the server along with a statement name to be created.
   */
  void doPrepare();

  /**
   * Utility to replace all occurances of a substring in a string
   *
   * @parm str The string to be modified.
   * @parm from The substring to be replaced.
   * @parm to The string to replace with.
   */
  void replaceAll(std::string& str, const std::string& from, const std::string& to) const;

  /**
   * Sets m_nbAffectedRows. Used during PostgresRset iteration.
   */
  void setAffectedRows(const uint64_t val) { m_nbAffectedRows = val; }

  /**
   * Called on DB error to generate error message and exception
   *
   * @param res The PGresult
   * @param prefix A prefix to place in the message thrown
   */
  [[noreturn]] void throwDB(const PGresult *res, const std::string &prefix);

  /**
   * Conditionally throws a DB related exception if the result status is not the expected one
   *
   * @param res The PGresult
   * @parm requiredStatus The required status
   * @param prefix A prefix to place in the message if it is thrown
   */
  void throwDBIfNotStatus(const PGresult *res, const ExecStatusType requiredStatus, const std::string &prefix);

  /**
   * Lock used to serialize access to the prepared statement and properies.
   */
  mutable threading::RWLock m_lock;

  /**
   * The SQL connection.
   */
  PostgresConn &m_conn;

  /**
   * The SQL of the statement with the bind variable format transformed.
   */
  std::string m_pgsql;

  /**
   * The prepared statement name as sent to the server.
   */
  std::string m_stmt;

  /**
   * The parameter count of prepared statement.
   */
  int m_nParams;

  /**
   * Used as an array of characeter pointers to C-string needed by libpq
   * to supply paramters on execution of a prepared statement.
   */
  std::vector<const char *> m_paramValuesPtrs;

  /**
   * Vector of strings. Used as the store of string data to which pointers will be
   * passed to libpq during statement execution.
   */
  std::vector<std::string> m_paramValues;

  /**
   * Used as storage for a row to send to PQputCopyData
   */
  std::string m_copyRow;

  /**
   * Associates column index to PostgresColumn objects
   */
  std::vector<PostgresColumn *> m_columnPtrs;

  /**
   * The number of rows affected by the last execution of this statement.
   */
  uint64_t m_nbAffectedRows;

}; // class PostgresStmt

} // namespace wrapper
} // namespace rdbms
} // namespace cta
