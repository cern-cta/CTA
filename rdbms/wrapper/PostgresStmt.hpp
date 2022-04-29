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

#include "common/threading/RWLock.hpp"
#include "common/threading/RWLockWrLocker.hpp"
#include "rdbms/wrapper/StmtWrapper.hpp"
#include "rdbms/wrapper/Postgres.hpp"
#include "rdbms/wrapper/PostgresConn.hpp"
#include "rdbms/wrapper/PostgresColumn.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

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
  void bindString(const std::string &paramName, const std::optional<std::string> &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint8(const std::string &paramName, const std::optional<uint8_t> &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint16(const std::string &paramName, const std::optional<uint16_t> &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint32(const std::string &paramName, const std::optional<uint32_t> &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint64(const std::string &paramName, const std::optional<uint64_t> &paramValue) override;

  /**
   * Binds an SQL parameter of type binary string (byte array).
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindBlob(const std::string &paramName, const std::string &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindDouble(const std::string &paramName, const std::optional<double> &paramValue) override;

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

  /**
   * Templated bind of an optional number.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  template <typename IntegerType> void bindInteger(const std::string &paramName,
    const std::optional<IntegerType> &paramValue) {
    threading::RWLockWrLocker locker(m_lock);

    try {
      const unsigned int paramIdx = getParamIdx(paramName); // starts from 1.

      if (paramIdx==0 || paramIdx>m_paramValues.size()) {
        throw exception::Exception(std::string("Bad index for paramName ") + paramName);
      }

      const unsigned int idx = paramIdx - 1;
      if (paramValue) {
        // we must not cause the vector m_paramValues to resize, otherwise the c-pointers can be invalidated
        m_paramValues[idx] = std::to_string(paramValue.value());
        m_paramValuesPtrs[idx] = m_paramValues[idx].c_str();
      } else {
        m_paramValues[idx].clear();
        m_paramValuesPtrs[idx] = nullptr;
      }
    } catch(exception::Exception &ex) {
      throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
                                 getSqlForException() + ": " + ex.getMessage().str());
    }
  }

}; // class PostgresStmt

} // namespace wrapper
} // namespace rdbms
} // namespace cta
