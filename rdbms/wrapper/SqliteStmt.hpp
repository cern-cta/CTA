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
#include "rdbms/wrapper/StmtWrapper.hpp"

#include <map>
#include <memory>
#include <stdint.h>
#include <sqlite3.h>

namespace cta {
namespace rdbms {
namespace wrapper {

class SqliteConn;
class SqliteRset;

/**
 * A convenience wrapper around an SQLite prepared statement.
 */
class SqliteStmt: public StmtWrapper {
public:

  /**
   * Constructor.
   *
   * This method is called by the SqliteStmt::createStmt() method and assumes
   * that a lock has already been taken on SqliteStmt::m_mutex;
   *
   * @param conn The database connection.
   * @param sql The SQL statement.
   */
  SqliteStmt(SqliteConn &conn, const std::string &sql);

  /**
   * Destructor.
   */
  ~SqliteStmt() override;

  /**
   * Clears the prepared statement so that it is ready to be reused.
   */
  void clear() override;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  void close() override;

  /**
   * Returns a pointer to the underlying prepared statement.
   *
   * This method throws an exception if the pointer to the underlying prepared
   * statement is nullptr.
   *
   * @return the underlying prepared statement.
   */
  sqlite3_stmt *get() const;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint8(const std::string &paramName, const optional<uint8_t> &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint16(const std::string &paramName, const optional<uint16_t> &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint32(const std::string &paramName, const optional<uint32_t> &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint64(const std::string &paramName, const optional<uint64_t> &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindDouble(const std::string &paramName, const optional<double> &paramValue) override;

  /** 
   * Binds an SQL parameter of type binary string (byte array).
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */ 
  void bindBlob(const std::string &paramName, const std::string &paramValue) override;

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
  void bindString(const std::string &paramName, const optional<std::string> &paramValue) override;

  /**
   * Executes the statement and returns the result set.
   *
   * @param autocommitMode The autocommit mode of the statement.
   * @return The result set.
   */
  std::unique_ptr<RsetWrapper> executeQuery() override;

  /**
   * Executes the statement.
   */
  void executeNonQuery() override;

  /**
   * Returns the number of rows affected by the last execution of this
   * statement.
   *
   * @return The number of affected rows.
   */
  uint64_t getNbAffectedRows() const override;

private:

  /**
   * Mutex used to serialize access to the prepared statement.
   */
  threading::Mutex m_mutex;

  /**
   * The SQL connection.
   */
  SqliteConn &m_conn;

  /**
   * The prepared statement.
   */
  sqlite3_stmt *m_stmt;

  /**
   * The number of rows affected by the last execution of this statement.
   */
  uint64_t m_nbAffectedRows;

  /**
   * @param autocommitMode The autocommit mode of the statement.
   *
   * @return true if autocommitMode is AUTOCOMMIT_ON, false if autocommitMode
   * is AUTOCOMMIT_OFF, else throws an exception.
   * @throw exception::Exception if autocommitMode is neither AUTOCOMMIT_ON
   * nor AUTOCOMMIT_OFF.
   */
  static bool autocommitModeToBool(const AutocommitMode autocommitMode);

}; // class SqlLiteStmt

} // namespace wrapper
} // namespace rdbms
} // namespace cta
