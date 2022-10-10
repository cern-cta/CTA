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

#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/threading/RWLockRdLocker.hpp"
#include "common/threading/RWLockWrLocker.hpp"

#include "rdbms/Conn.hpp"
#include "rdbms/wrapper/PostgresConn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"

#include <stdio.h>
#include <sstream>
#include <exception>

namespace cta {
namespace rdbms {
namespace wrapper {


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresConn::PostgresConn(const std::string &conninfo)
  : m_pgsqlConn(nullptr), m_asyncInProgress(false), m_nStmts(0) {

  // establish the connection and create the PGconn data structure

  m_pgsqlConn = PQconnectdb(conninfo.c_str());

  if (PQstatus(m_pgsqlConn) != CONNECTION_OK) {
    const std::string pqmsgstr = PQerrorMessage(m_pgsqlConn);
    PQfinish(m_pgsqlConn);
    m_pgsqlConn = nullptr;
    throw exception::Exception(std::string(__FUNCTION__) + " connection failed: " + pqmsgstr);
  }

  const int sVer = PQserverVersion(m_pgsqlConn);
  if (sVer < 90500) {
    PQfinish(m_pgsqlConn);
    m_pgsqlConn = nullptr;
    const int maj = (sVer/10000) % 100;
    const int min = (sVer/100) % 100;
    const int rel = sVer % 100;
    std::ostringstream msg;
    msg << maj << "." << min << "." << rel;
    throw exception::Exception(std::string(__FUNCTION__) +
      " requires postgres server version be at least 9.5: the server is " + msg.str());
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
void PostgresConn::commit()
{
  threading::RWLockWrLocker locker(m_lock);

  if (!isOpenAssumeLocked()) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Connection is closed");
  }

  if (isAsyncInProgress()) {
    // if a Conn object is freeded and its ConnAndStmts object returned to its ConnPool while
    // async is in progress, this exception will be thrown during the commit() in returnConn.
    // The ConnAndStmts will be destroyed, and consequently this wrapper::Conn also
    throw exception::Exception(std::string(__FUNCTION__) + " can not execute sql, another query is in progress");
  }

  Postgres::Result res(PQexec(m_pgsqlConn, "COMMIT"));
  throwDBIfNotStatus(res.get(), PGRES_COMMAND_OK, std::string(__FUNCTION__) + " problem committing the DB transaction");
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
std::unique_ptr<StmtWrapper> PostgresConn::createStmt(const std::string &sql) {

  threading::RWLockRdLocker locker(m_lock);

  try {
    if(!isOpenAssumeLocked()) {
      throw exception::Exception("Connection is closed");
    }

  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }

  // PostgresStmt constructor assumes conneciton is rd locked
  return std::make_unique<PostgresStmt>(*this, sql);
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void PostgresConn::executeNonQuery(const std::string &sql) {
  threading::RWLockWrLocker locker(m_lock);

  if(!isOpenAssumeLocked()) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Connection is closed");
  }

  Postgres::Result res(PQexec(m_pgsqlConn, sql.c_str()));

  // If this method is used for a query we will get PGRES_TUPLES_OK here
  throwDBIfNotStatus(res.get(), PGRES_COMMAND_OK, std::string(__FUNCTION__) + " problem executing " + sql);
}

//------------------------------------------------------------------------------
// getAutocommitMode
//------------------------------------------------------------------------------
AutocommitMode PostgresConn::getAutocommitMode() const noexcept{
  return AutocommitMode::AUTOCOMMIT_ON;
}

//------------------------------------------------------------------------------
// getSequenceNames
//------------------------------------------------------------------------------
std::list<std::string> PostgresConn::getSequenceNames() {
  std::list<std::string> names;

  threading::RWLockWrLocker locker(m_lock);

  try {
    if (!isOpenAssumeLocked()) {
      throw exception::Exception("Connection is closed");
    }

    if (isAsyncInProgress()) {
      throw exception::Exception("can not execute sql, another query is in progress");
    }

    Postgres::Result res(PQexec(m_pgsqlConn,
       "SELECT c.relname AS SEQUENCE_NAME FROM pg_class c "
         "WHERE c.relkind = 'S' ORDER BY SEQUENCE_NAME"
       ));

    throwDBIfNotStatus(res.get(), PGRES_TUPLES_OK, "Listing Sequences in the DB");

    const int num_fields = PQnfields(res.get());
    if (1 != num_fields) {
      throw exception::Exception("number fields wrong during list sequences: Got " + std::to_string(num_fields));
    }

    for(int i=0;i<PQntuples(res.get());++i) {
      std::string name = PQgetvalue(res.get(), i, 0);
      utils::toUpper(name);
      names.push_back(name);
    }
  } catch(exception::LostDatabaseConnection &ex) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " detected lost connection: " + ex.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }

  return names;
}

//------------------------------------------------------------------------------
// getColumns
//------------------------------------------------------------------------------
std::map<std::string, std::string> PostgresConn::getColumns(const std::string &tableName) {
  try {
    std::map<std::string, std::string> columnNamesAndTypes;
    auto lowercaseTableName = tableName;
    utils::toLower(lowercaseTableName); // postgres work with lowercase
    const char *const sql =
      "SELECT "
        "COLUMN_NAME, "
        "DATA_TYPE "
      "FROM "
        "INFORMATION_SCHEMA.COLUMNS "
      "WHERE "
        "TABLE_NAME = :TABLE_NAME";

    auto stmt = createStmt(sql);
    stmt->bindString(":TABLE_NAME", lowercaseTableName);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("COLUMN_NAME");
      auto type = rset->columnOptionalString("DATA_TYPE");
      if(name && type) {
        utils::toUpper(name.value());
        utils::toUpper(type.value());
        if ("CHARACTER VARYING" == type.value()) {
          type = "VARCHAR" ;
        } else if ("CHARACTER" == type.value()) {
          type = "CHAR";
        }
        columnNamesAndTypes.insert(std::make_pair(name.value(), type.value()));
      }
    }

    return columnNamesAndTypes;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getTableNames
//------------------------------------------------------------------------------
std::list<std::string> PostgresConn::getTableNames() {
  std::list<std::string> names;

  threading::RWLockWrLocker locker(m_lock);

  try {
    if (!isOpenAssumeLocked()) {
      throw exception::Exception("Connection is closed");
    }

    if (isAsyncInProgress()) {
      throw exception::Exception("can not execute sql, another query is in progress");
    }

    Postgres::Result res(PQexec(m_pgsqlConn,
      "SELECT "
        "tablename "
      "FROM "
        "pg_catalog.pg_tables "
      "WHERE "
        "pg_catalog.pg_tables.schemaname = current_schema() "
      "ORDER BY tablename"
    ));

    throwDBIfNotStatus(res.get(), PGRES_TUPLES_OK, "Listing table names in the DB");

    const int num_fields = PQnfields(res.get());
    if (1 != num_fields) {
      throw exception::Exception("number fields wrong during list tables: Got " + std::to_string(num_fields));
    }

    for(int i=0;i<PQntuples(res.get());++i) {
      std::string name = PQgetvalue(res.get(), i, 0);
      utils::toUpper(name);
      names.push_back(name);
    }
  } catch(exception::LostDatabaseConnection &ex) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " detected lost connection: " + ex.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }

  return names;
}

//------------------------------------------------------------------------------
// getIndexNames
//------------------------------------------------------------------------------
std::list<std::string> PostgresConn::getIndexNames() {
  try {
    std::list<std::string> names;
    const char *const sql =
      "SELECT "
        "INDEXNAME "
      "FROM "
        "PG_INDEXES "
      "WHERE "
        "SCHEMANAME = 'public' "
      "ORDER BY "
        "INDEXNAME";
    auto stmt = createStmt(sql);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("INDEXNAME");
      if(name) {
        utils::toUpper(name.value());
        names.push_back(name.value());
      }
    }

    return names;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
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
std::list<std::string> PostgresConn::getParallelTableNames(){
  return std::list<std::string>();
}
//------------------------------------------------------------------------------
// getConstraintNames
//------------------------------------------------------------------------------
std::list<std::string> PostgresConn::getConstraintNames(const std::string& tableName){
  try {
    std::list<std::string> names;
    const char *const sql =
      "SELECT "
        "CON.CONNAME AS CONSTRAINT_NAME "
      "FROM "
        "PG_CATALOG.PG_CONSTRAINT CON "
      "INNER JOIN PG_CATALOG.PG_CLASS REL "
        "ON REL.OID=CON.CONRELID "
      "INNER JOIN PG_CATALOG.PG_NAMESPACE NSP "
        "ON NSP.OID = CONNAMESPACE "
      "WHERE "
        "REL.RELNAME=:TABLE_NAME";
    auto stmt = createStmt(sql);
    std::string localTableName = tableName;
    utils::toLower(localTableName);
    stmt->bindString(":TABLE_NAME",localTableName);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("CONSTRAINT_NAME");
      if(name) {
        utils::toUpper(name.value());
        names.push_back(name.value());
      }
    }

    return names;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
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
  return std::list<std::string>();
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
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Connection is closed");
  }

  if (isAsyncInProgress()) {
    throw exception::Exception(std::string(__FUNCTION__) + " can not execute sql, another query is in progress");
  }

  Postgres::Result res(PQexec(m_pgsqlConn, "ROLLBACK"));
  throwDBIfNotStatus(res.get(), PGRES_COMMAND_OK, std::string(__FUNCTION__) + " problem rolling back the DB transaction");
}

//------------------------------------------------------------------------------
// setAutocommitMode
//------------------------------------------------------------------------------
void PostgresConn::setAutocommitMode(const AutocommitMode autocommitMode) {
  if(AutocommitMode::AUTOCOMMIT_OFF == autocommitMode) {
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
void PostgresConn::deallocateStmt(const std::string &stmt) {
  // assumes connection lock is held

  std::ostringstream s;
  s << "DEALLOCATE " << stmt;

  Postgres::Result res(PQexec(m_pgsqlConn, s.str().c_str()));
  throwDBIfNotStatus(res.get(), PGRES_COMMAND_OK, std::string(__FUNCTION__) + " failed to DEALLOCATE statement " + stmt);
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
void PostgresConn::noticeProcessor(void *arg, const char *message) {
  //fprintf(stderr, "%s", message);
}

//------------------------------------------------------------------------------
// throwDBIfNotStatus
//------------------------------------------------------------------------------
void PostgresConn::throwDBIfNotStatus(const PGresult *res,
            const ExecStatusType requiredStatus, const std::string &prefix) {
  // assumes connection wr lock held
  if (PQresultStatus(res) != requiredStatus) {
    try {
      Postgres::ThrowInfo(m_pgsqlConn,res,prefix);
    } catch(exception::LostDatabaseConnection &) {
      try {
        closeAssumeLocked();
      } catch(std::exception &) {
      }
      throw;
    }
  }
}

} // namespace wrapper
} // namespace rdbms
} // namespace cta
