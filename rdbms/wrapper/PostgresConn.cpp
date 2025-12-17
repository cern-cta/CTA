/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/wrapper/PostgresConn.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/process/threading/RWLockRdLocker.hpp"
#include "common/process/threading/RWLockWrLocker.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"

#include <exception>
#include <sstream>
#include <stdio.h>

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresConn::PostgresConn(const rdbms::Login& login) : m_dbNamespace(login.dbNamespace) {
  // establish the connection and create the PGconn data structure

  m_pgsqlConn = PQconnectdb(login.database.c_str());

  if (PQstatus(m_pgsqlConn) != CONNECTION_OK) {
    const std::string pqmsgstr = PQerrorMessage(m_pgsqlConn);
    PQfinish(m_pgsqlConn);
    m_pgsqlConn = nullptr;
    throw exception::Exception("Connection failed: " + pqmsgstr);
  }
  if (const int sVer = PQserverVersion(m_pgsqlConn); sVer < 90500) {
    PQfinish(m_pgsqlConn);
    m_pgsqlConn = nullptr;
    const int maj = (sVer / 10000) % 100;
    const int min = (sVer / 100) % 100;
    const int rel = sVer % 100;
    std::ostringstream msg;
    msg << maj << "." << min << "." << rel;
    throw exception::Exception("Postgres server version must be at least 9.5: the server is " + msg.str());
  }

  PQsetNoticeProcessor(m_pgsqlConn, PostgresConn::noticeProcessor, nullptr);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
PostgresConn::~PostgresConn() {
  try {
    close();
  } catch (...) {
    // Destructor should not throw any exceptions
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void PostgresConn::close() {
  threading::RWLockWrLocker locker(m_lock);

  closeAssumeLocked();
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void PostgresConn::commit() {
  threading::RWLockWrLocker locker(m_lock);

  if (!isOpenAssumeLocked()) {
    throw exception::Exception("Connection is closed");
  }

  if (isAsyncInProgress()) {
    // if a Conn object is freeded and its ConnAndStmts object returned to its ConnPool while
    // async is in progress, this exception will be thrown during the commit() in returnConn.
    // The ConnAndStmts will be destroyed, and consequently this wrapper::Conn also
    throw exception::Exception("Cannot execute SQL query, another query is in progress");
  }

  if (PQTRANS_IDLE == PQtransactionStatus(m_pgsqlConn)) {
    // Commit is always called when returning a conneciton to the pool.
    // Postgres logs a warning if we commit without a transaction. To avoid
    // this a check is made, using client side server status tracking information:
    // if we're not in a transaction return, otherwise attempt the commit.
    return;
  }
  Postgres::Result res(PQexec(m_pgsqlConn, "COMMIT"));
  throwDBIfNotStatus(res.get(), PGRES_COMMAND_OK, "Problem committing the DB transaction");
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
std::unique_ptr<StmtWrapper> PostgresConn::createStmt(const std::string& sql) {
  threading::RWLockRdLocker locker(m_lock);

  if (!isOpenAssumeLocked()) {
    throw exception::Exception("Connection is closed");
  }

  // PostgresStmt constructor assumes conneciton is rd locked
  return std::make_unique<PostgresStmt>(*this, sql);
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void PostgresConn::executeNonQuery(const std::string& sql) {
  threading::RWLockWrLocker locker(m_lock);

  if (!isOpenAssumeLocked()) {
    throw exception::Exception("Connection is closed");
  }

  Postgres::Result res(PQexec(m_pgsqlConn, sql.c_str()));

  // If this method is used for a query we will get PGRES_TUPLES_OK here
  throwDBIfNotStatus(res.get(), PGRES_COMMAND_OK, "Problem executing " + sql);
}

//------------------------------------------------------------------------------
// getAutocommitMode
//------------------------------------------------------------------------------
AutocommitMode PostgresConn::getAutocommitMode() const noexcept {
  return AutocommitMode::AUTOCOMMIT_ON;
}

//------------------------------------------------------------------------------
// getSequenceNames
//------------------------------------------------------------------------------
std::list<std::string> PostgresConn::getSequenceNames() {
  std::list<std::string> names;

  threading::RWLockWrLocker locker(m_lock);

  if (!isOpenAssumeLocked()) {
    throw exception::Exception("Connection is closed");
  }

  if (isAsyncInProgress()) {
    throw exception::Exception("can not execute sql, another query is in progress");
  }

  Postgres::Result res(PQexec(m_pgsqlConn, R"SQL(
      SELECT c.relname AS SEQUENCE_NAME FROM pg_class c
        WHERE c.relkind = 'S' ORDER BY SEQUENCE_NAME
    )SQL"));

  throwDBIfNotStatus(res.get(), PGRES_TUPLES_OK, "Listing Sequences in the DB");

  if (const int num_fields = PQnfields(res.get()); 1 != num_fields) {
    throw exception::Exception("number fields wrong during list sequences: Got " + std::to_string(num_fields));
  }

  for (int i = 0; i < PQntuples(res.get()); ++i) {
    std::string name = PQgetvalue(res.get(), i, 0);
    utils::toUpper(name);
    names.push_back(name);
  }

  return names;
}

//------------------------------------------------------------------------------
// getColumns
//------------------------------------------------------------------------------
std::map<std::string, std::string, std::less<>> PostgresConn::getColumns(const std::string& tableName) {
  std::map<std::string, std::string, std::less<>> columnNamesAndTypes;
  auto lowercaseTableName = tableName;
  utils::toLower(lowercaseTableName);  // postgres work with lowercase
  const char* const sql = R"SQL(
    SELECT
      COLUMN_NAME,
      DATA_TYPE
    FROM
      INFORMATION_SCHEMA.COLUMNS
    WHERE
      TABLE_NAME = :TABLE_NAME
  )SQL";

  auto stmt = createStmt(sql);
  stmt->bindString(":TABLE_NAME", lowercaseTableName);
  auto rset = stmt->executeQuery();
  while (rset->next()) {
    auto name = rset->columnOptionalString("COLUMN_NAME");
    auto type = rset->columnOptionalString("DATA_TYPE");
    if (name && type) {
      utils::toUpper(name.value());
      utils::toUpper(type.value());
      if ("CHARACTER VARYING" == type.value()) {
        type = "VARCHAR";
      } else if ("CHARACTER" == type.value()) {
        type = "CHAR";
      }
      columnNamesAndTypes.insert(std::make_pair(name.value(), type.value()));
    }
  }

  return columnNamesAndTypes;
}

//------------------------------------------------------------------------------
// getTableNames
//------------------------------------------------------------------------------
std::list<std::string> PostgresConn::getTableNames() {
  std::list<std::string> names;

  threading::RWLockWrLocker locker(m_lock);

  if (!isOpenAssumeLocked()) {
    throw exception::Exception("Connection is closed");
  }

  if (isAsyncInProgress()) {
    throw exception::Exception("can not execute sql, another query is in progress");
  }

  Postgres::Result res(PQexec(m_pgsqlConn, R"SQL(
    SELECT
      tablename
    FROM
      pg_catalog.pg_tables
    WHERE
      pg_catalog.pg_tables.schemaname = current_schema()
    ORDER BY tablename
  )SQL"));

  throwDBIfNotStatus(res.get(), PGRES_TUPLES_OK, "Listing table names in the DB");

  if (const int num_fields = PQnfields(res.get()); 1 != num_fields) {
    throw exception::Exception("number fields wrong during list tables: Got " + std::to_string(num_fields));
  }

  for (int i = 0; i < PQntuples(res.get()); ++i) {
    std::string name = PQgetvalue(res.get(), i, 0);
    utils::toUpper(name);
    names.push_back(name);
  }

  return names;
}

//------------------------------------------------------------------------------
// getIndexNames
//------------------------------------------------------------------------------
std::list<std::string> PostgresConn::getIndexNames() {
  std::list<std::string> names;
  const char* const sql = R"SQL(
    SELECT
      INDEXNAME
    FROM
      PG_INDEXES
    WHERE
      SCHEMANAME = 'public'
    ORDER BY
      INDEXNAME
  )SQL";
  auto stmt = createStmt(sql);
  auto rset = stmt->executeQuery();
  while (rset->next()) {
    auto name = rset->columnOptionalString("INDEXNAME");
    if (name) {
      utils::toUpper(name.value());
      names.push_back(name.value());
    }
  }

  return names;
}

//------------------------------------------------------------------------------
// getTriggerNames
//------------------------------------------------------------------------------
std::list<std::string> PostgresConn::getTriggerNames() {
  return std::list<std::string>();
}

//------------------------------------------------------------------------------
// getParallelTableNames
//------------------------------------------------------------------------------
std::list<std::string> PostgresConn::getParallelTableNames() {
  return std::list<std::string>();
}

//------------------------------------------------------------------------------
// getConstraintNames
//------------------------------------------------------------------------------
std::list<std::string> PostgresConn::getConstraintNames(const std::string& tableName) {
  std::list<std::string> names;
  const char* const sql = R"SQL(
    SELECT
      CON.CONNAME AS CONSTRAINT_NAME
    FROM
      PG_CATALOG.PG_CONSTRAINT CON
    INNER JOIN PG_CATALOG.PG_CLASS REL
      ON REL.OID=CON.CONRELID
    INNER JOIN PG_CATALOG.PG_NAMESPACE NSP
      ON NSP.OID = CONNAMESPACE
    WHERE
      REL.RELNAME=:TABLE_NAME
  )SQL";
  auto stmt = createStmt(sql);
  std::string localTableName = tableName;
  utils::toLower(localTableName);
  stmt->bindString(":TABLE_NAME", localTableName);
  auto rset = stmt->executeQuery();
  while (rset->next()) {
    auto name = rset->columnOptionalString("CONSTRAINT_NAME");
    if (name) {
      utils::toUpper(name.value());
      names.push_back(name.value());
    }
  }

  return names;
}

//------------------------------------------------------------------------------
// getStoredProcedureNames
//------------------------------------------------------------------------------
std::list<std::string> PostgresConn::getStoredProcedureNames() {
  return std::list<std::string>();
}

//------------------------------------------------------------------------------
// getSynonymNames
//------------------------------------------------------------------------------
std::list<std::string> PostgresConn::getSynonymNames() {
  return std::list<std::string>();
}

//------------------------------------------------------------------------------
// getTypeNames
//------------------------------------------------------------------------------
std::list<std::string> PostgresConn::getTypeNames() {
  std::list<std::string> names;
  const char* const sql = R"SQL(
    SELECT
      TYPNAME
    FROM
      PG_TYPE
    WHERE typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
  )SQL";
  auto stmt = createStmt(sql);
  auto rset = stmt->executeQuery();
  while (rset->next()) {
    auto name = rset->columnOptionalString("TYPNAME");
    names.push_back(name.value());
  }
  return names;
}

//------------------------------------------------------------------------------
// getTypeNames
//------------------------------------------------------------------------------
std::list<std::string> PostgresConn::getViewNames() {
  std::list<std::string> names;
  const char* const sql = R"SQL(
    SELECT
      TABLE_NAME
    FROM
      INFORMATION_SCHEMA.VIEWS
    WHERE table_schema = 'public'
  )SQL";
  auto stmt = createStmt(sql);
  auto rset = stmt->executeQuery();
  while (rset->next()) {
    auto name = rset->columnOptionalString("TABLE_NAME");
    names.push_back(name.value());
  }
  return names;
}

//------------------------------------------------------------------------------
// isOpen
//------------------------------------------------------------------------------
bool PostgresConn::isOpen() const {
  threading::RWLockRdLocker locker(m_lock);
  return isOpenAssumeLocked();
}

//------------------------------------------------------------------------------
// rollback
//------------------------------------------------------------------------------
void PostgresConn::rollback() {
  threading::RWLockWrLocker locker(m_lock);

  if (!isOpenAssumeLocked()) {
    throw exception::Exception("Connection is closed");
  }

  if (isAsyncInProgress()) {
    throw exception::Exception("Cannot execute sql, another query is in progress");
  }

  Postgres::Result res(PQexec(m_pgsqlConn, "ROLLBACK"));
  throwDBIfNotStatus(res.get(), PGRES_COMMAND_OK, "Problem rolling back the DB transaction");
}

//------------------------------------------------------------------------------
// setAutocommitMode
//------------------------------------------------------------------------------
void PostgresConn::setAutocommitMode(const AutocommitMode autocommitMode) {
  if (AutocommitMode::AUTOCOMMIT_OFF == autocommitMode) {
    throw rdbms::Conn::AutocommitModeNotSupported("Failed to set autocommit mode to AUTOCOMMIT_OFF: PostgresConn only"
                                                  " supports AUTOCOMMIT_ON");
  }
}

//------------------------------------------------------------------------------
// closeAssumeLocked
//------------------------------------------------------------------------------
void PostgresConn::closeAssumeLocked() {
  // assumes wr locked
  if (isOpenAssumeLocked()) {
    // this is an implicit rollback if there is a transaction ongoing
    PQfinish(m_pgsqlConn);
    m_pgsqlConn = nullptr;
  }
}

//------------------------------------------------------------------------------
// decallocateStmt
//------------------------------------------------------------------------------
void PostgresConn::deallocateStmt(const std::string& stmt) {
  // assumes connection lock is held

  std::ostringstream s;
  s << "DEALLOCATE " << stmt;

  Postgres::Result res(PQexec(m_pgsqlConn, s.str().c_str()));
  throwDBIfNotStatus(res.get(), PGRES_COMMAND_OK, "Failed to DEALLOCATE statement " + stmt);
}

//------------------------------------------------------------------------------
// isOpenAssumeLocked
//------------------------------------------------------------------------------

bool PostgresConn::isOpenAssumeLocked() const {
  // assumes rd locked
  return CONNECTION_OK == PQstatus(m_pgsqlConn);
}

//------------------------------------------------------------------------------
// nextStmtName
//------------------------------------------------------------------------------
std::string PostgresConn::nextStmtName() {
  // assumes we have connection object wr lock
  ++m_nStmts;
  return "s" + std::to_string(m_nStmts);
}

//------------------------------------------------------------------------------
// noticeProcessor
//------------------------------------------------------------------------------
void PostgresConn::noticeProcessor([[maybe_unused]] void* arg, [[maybe_unused]] const char* message) {
  //fprintf(stderr, "%s", message);
}

//------------------------------------------------------------------------------
// throwDBIfNotStatus
//------------------------------------------------------------------------------
void PostgresConn::throwDBIfNotStatus(const PGresult* res,
                                      const ExecStatusType requiredStatus,
                                      const std::string& prefix) {
  // assumes connection wr lock held
  if (PQresultStatus(res) != requiredStatus) {
    try {
      Postgres::ThrowInfo(m_pgsqlConn, res, prefix);
    } catch (exception::LostDatabaseConnection& ldbex) {
      closeAssumeLocked();
      throw;
    }
  }
}

//------------------------------------------------------------------------------
// getDbNamespace
//------------------------------------------------------------------------------
std::string PostgresConn::getDbNamespace() const {
  return m_dbNamespace;
}

}  // namespace cta::rdbms::wrapper
