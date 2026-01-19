/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/AutocommitMode.hpp"
#include "rdbms/Rset.hpp"

#include <list>
#include <memory>
#include <mutex>
#include <optional>

namespace cta::rdbms {

namespace wrapper {
class StmtWrapper;
}

class StmtPool;

/**
 * A smart database statement that will automatically return the underlying
 * database statement to its parent database connection when it goes out of
 * scope.
 */
class Stmt {
public:
  /**
   * Constructor.
   */
  Stmt();

  /**
   * Constructor.
   *
   * @param stmt The database statement.
   * @param stmtPool The database statement pool to which the m_stmt should be returned.
   */
  Stmt(std::unique_ptr<wrapper::StmtWrapper> stmt, StmtPool& stmtPool);

  /**
   * Deletion of the copy constructor.
   */
  Stmt(Stmt&) = delete;

  /**
   * Move constructor.
   *
   * @param other The other object.
   */
  Stmt(Stmt&& other) noexcept;

  /**
   * Destructor.
   *
   * Returns the database statement back to its connection.
   */
  ~Stmt() noexcept;

  /**
   * Reset the statment by returning any owned statement back to its statment
   * pool.
   *
   * This method is idempotent.
   */
  void reset() noexcept;

  /**
   * Deletion of the copy assignment operator.
   */
  Stmt& operator=(const Stmt&) = delete;

  /**
   * Move assignment operator.
   *
   * @param rhs The object on the right-hand side of the operator.
   * @return This object.
   */
  Stmt& operator=(Stmt&& rhs) noexcept;

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  const std::string& getSql() const;

  /**
   * Returns the index of the specified SQL parameter.
   *
   * @param paramName The name of the SQL parameter.
   * @return The index of the SQL parameter.
   */
  uint32_t getParamIdx(const std::string& paramName) const;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint8(const std::string& paramName, const std::optional<uint8_t>& paramValue);

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint16(const std::string& paramName, const std::optional<uint16_t>& paramValue);

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint32(const std::string& paramName, const std::optional<uint32_t>& paramValue);

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint64(const std::string& paramName, const std::optional<uint64_t>& paramValue);

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindDouble(const std::string& paramName, const std::optional<double>& paramValue);

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindBool(const std::string& paramName, const std::optional<bool>& paramValue);

  /**
   * Binds an SQL parameter of type binary blob.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindBlob(const std::string& paramName, const std::string& paramValue);

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
  void bindString(const std::string& paramName, const std::optional<std::string>& paramValue);

  /**
   *  Clears the prepared statement so that it is ready to be reused, without the need to bind new variables.
   */
  void resetQuery();

  /**
   *  Executes the statement and returns the result set.
   *
   *  @return The result set.
   */
  Rset executeQuery();

  /**
   * Executes the statement.
   */
  void executeNonQuery();

  /**
   * Returns the number of rows affected by the last execution of this
   * statement.
   *
   * @return The number of affected rows.
   */
  uint64_t getNbAffectedRows() const;

  /**
   * Returns a reference to the underlying statement object that is not pool
   * aware.
   */
  wrapper::StmtWrapper& getStmt();

  /**
   * @brief Get the query type string describing the query type.
   *
   * This should return the OpenTelemetry semantic convention attribute
   * `db.query.summary`. It identifies the query type being executed.
   *
   * @return A string representing the query type.
   */
  const std::string getDbQuerySummary() const;

  /**
   * @brief Set the query type string describing the query type.
   *
   * This should set m_queryType to be later used for the OpenTelemetry
   * semantic convention attribute `db.query.summary`.
   * It identifies the query type being executed.
   */
  void setDbQuerySummary(const std::string& optQuerySummary);

private:
  /**
   * The database statement.
   */
  std::unique_ptr<wrapper::StmtWrapper> m_stmt;

  /**
   * The database statement pool to which the m_stmt should be returned.
   */
  StmtPool* m_stmtPool;

};  // class Stmt

}  // namespace cta::rdbms
