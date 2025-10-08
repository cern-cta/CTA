/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/threading/Mutex.hpp"
#include "rdbms/wrapper/StmtWrapper.hpp"

#include <map>
#include <memory>
#include <stdint.h>
#include <sqlite3.h>

namespace cta::rdbms::wrapper {

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
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindDouble(const std::string &paramName, const std::optional<double> &paramValue) override;

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
  void bindString(const std::string &paramName, const std::optional<std::string> &paramValue) override;

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

  /**
   * @brief Get the database system identifier.
   *
   * This should return the OpenTelemetry semantic convention attribute
   * `db.system`. It identifies which kind of database management system
   * (DBMS) is in use.
   *
   * @return The string "sqlite".
   */
  std::string getDbSystemName() const override;

  /**
   * @brief Get the logical database namespace.
   *
   * This should return the OpenTelemetry semantic convention attribute
   * `db.namespace`. It identifies the logical database, schema, or
   * namespace where the operation is executed.
   *
   * @return A string representing the database namespace.
   */
  std::string getDbNamespace() const override;

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

} // namespace cta::rdbms::wrapper
