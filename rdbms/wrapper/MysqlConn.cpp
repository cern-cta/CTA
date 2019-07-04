/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
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

#include "common/exception/Exception.hpp"
#include "common/make_unique.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/wrapper/Mysql.hpp"
#include "rdbms/wrapper/MysqlConn.hpp"
#include "rdbms/wrapper/MysqlStmt.hpp"

#include <mysql.h>

#include <iostream>
#include <stdexcept>
#include <string>

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MysqlConn::MysqlConn(const std::string& host, 
                     const std::string& user, 
                     const std::string& passwd,
                     const std::string& db,
                     unsigned int port)
  : m_mysqlConn(NULL) {

  // create the MYSQL data structure

  m_mysqlConn = mysql_init(NULL);

  if (m_mysqlConn == NULL) {
    unsigned int rc = mysql_errno(m_mysqlConn);
    std::string msg = mysql_error(m_mysqlConn);
    throw exception::Exception(std::string(" errno: ") + std::to_string(rc) + " " +  msg);
  }

  // we can use mysql_options() to change the connect options

  // connect

  if (mysql_real_connect(m_mysqlConn, host.c_str(),
                         user.c_str(), passwd.c_str(), db.c_str(), port, 
                         NULL, CLIENT_FOUND_ROWS) == NULL) {
    unsigned int rc = mysql_errno(m_mysqlConn);
    std::string msg = mysql_error(m_mysqlConn);
    throw exception::Exception(std::string(" errno: ") + std::to_string(rc) + " " +  msg);
  }

  // disable auto commit
  bool rc = mysql_autocommit(m_mysqlConn, 0); // mode is 1, off if mode is 0.
  if (rc) {
    std::string msg = "Can not turn off autocommit mode.";
    throw exception::Exception(msg);
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
MysqlConn::~MysqlConn() {
  try {
    close();
  } catch (...) {
    // Destructor should not throw any exceptions
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void MysqlConn::close() {
  threading::MutexLocker locker(m_mutex);

  if (isOpen()) {
    mysql_close(m_mysqlConn);
    m_mysqlConn = nullptr;
  }
}

//------------------------------------------------------------------------------
// setAutocommitMode
//------------------------------------------------------------------------------
void MysqlConn::setAutocommitMode(const AutocommitMode autocommitMode) {
  if(AutocommitMode::AUTOCOMMIT_OFF == autocommitMode) {
    throw rdbms::Conn::AutocommitModeNotSupported("Failed to set autocommit mode to AUTOCOMMIT_OFF: MysqlConn only"
      " supports AUTOCOMMIT_ON");
  }
}

//------------------------------------------------------------------------------
// getAutocommitMode
//------------------------------------------------------------------------------
AutocommitMode MysqlConn::getAutocommitMode() const noexcept{
  return AutocommitMode::AUTOCOMMIT_ON;
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void MysqlConn::executeNonQuery(const std::string &sql) {
  try {
    threading::MutexLocker locker(m_mutex);

    if(!isOpen()) {
      throw exception::Exception("Connection is closed");
    }

    const auto translatedSql = Mysql::translate_it(sql);

    if (mysql_real_query(m_mysqlConn, translatedSql.c_str(), translatedSql.length())) {
      unsigned int rc = mysql_errno(m_mysqlConn);
      std::string msg = mysql_error(m_mysqlConn);
      throw exception::Exception(std::string(" errno: ") + std::to_string(rc) + " " +  msg);
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
std::unique_ptr<StmtWrapper> MysqlConn::createStmt(const std::string &sql) {

  try {
    threading::MutexLocker locker(m_mutex);

    if(!isOpen()) {
      throw exception::Exception("Connection is closed");
    }

    return cta::make_unique<MysqlStmt>(*this, sql);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
void MysqlConn::execute(const std::string &sql) {
  if (mysql_real_query(m_mysqlConn, sql.c_str(), sql.length())) {
    unsigned int rc = mysql_errno(m_mysqlConn);
    std::string msg = mysql_error(m_mysqlConn);
    throw exception::Exception(std::string(" errno: ") + std::to_string(rc) + " " +  msg);
  }
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void MysqlConn::commit() 
{
  threading::MutexLocker locker(m_mutex);

  if (!isOpen()) {
    throw exception::Exception("Connection is closed");
  }

  my_bool isCommit = mysql_commit(m_mysqlConn);

  if (isCommit) {
    throw exception::Exception(std::string(__FUNCTION__) + " commit failed.");
  }
}

//------------------------------------------------------------------------------
// rollback
//------------------------------------------------------------------------------
void MysqlConn::rollback() {
  threading::MutexLocker locker(m_mutex);

  if (!isOpen()) {
    throw exception::Exception("Connection is closed");
  }

  my_bool isCommit = mysql_rollback(m_mysqlConn);

  if (!isCommit) {
    throw exception::Exception(std::string(__FUNCTION__) + " rollback failed.");
  }
}

//------------------------------------------------------------------------------
// getColumns
//------------------------------------------------------------------------------
std::map<std::string, std::string> MysqlConn::getColumns(const std::string &tableName) {
 try {
    std::map<std::string, std::string> columnNamesAndTypes;
    const char *const sql =
      "SELECT "
        "COLUMN_NAME, "
        "DATA_TYPE "
      "FROM "
        "INFORMATION_SCHEMA.COLUMNS "
      "WHERE "
        "TABLE_NAME = :TABLE_NAME";

    auto stmt = createStmt(sql);
    stmt->bindString(":TABLE_NAME", tableName);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("COLUMN_NAME");
      auto type = rset->columnOptionalString("DATA_TYPE");
      if(name && type) {
        utils::toUpper(type.value());
        if ("DECIMAL" == type.value()) {
          type = "NUMERIC" ;
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
std::list<std::string> MysqlConn::getTableNames() {
  std::list<std::string> names;
  SmartMYSQL_RES result(mysql_list_tables(m_mysqlConn, NULL));

  if (result) {
    // try to retrieve

    int num_fields = mysql_num_fields(result.get());

    if (num_fields != 1) {
      throw exception::Exception(std::string(__FUNCTION__) + " num fields (list tables): " + std::to_string(num_fields));
    }

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result.get()))) { 
      std::string name = row[0]; // only one field
      names.push_back(name);
    }
  
  }

  return names;
}

//------------------------------------------------------------------------------
// getIndexNames
//------------------------------------------------------------------------------
std::list<std::string> MysqlConn::getIndexNames() {
  try {
    std::list<std::string> names;
    const char *const sql =
      "SELECT DISTINCT "
        "INDEX_NAME "
      "FROM "
        "INFORMATION_SCHEMA.STATISTICS "
      "WHERE "
        "Non_unique=1 "
      "ORDER BY "
        "INDEX_NAME";
    auto stmt = createStmt(sql);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("INDEX_NAME");
      if(name) {
        names.push_back(name.value());
      }
    }

    return names;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// isOpen
//------------------------------------------------------------------------------
bool MysqlConn::isOpen() const {
  return NULL != m_mysqlConn;
}

//------------------------------------------------------------------------------
// getSequenceNames
//------------------------------------------------------------------------------
std::list<std::string> MysqlConn::getSequenceNames() {
  return std::list<std::string>();
}

//------------------------------------------------------------------------------
// getTriggerNames
//------------------------------------------------------------------------------
std::list<std::string> MysqlConn::getTriggerNames() {
  try {
    std::list<std::string> names;
    const char *const sql =
      "SELECT "
        "TRIGGER_NAME "
      "FROM "
        "INFORMATION_SCHEMA.TRIGGERS "
      "ORDER BY "
        "TRIGGER_NAME";
    auto stmt = createStmt(sql);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("TRIGGER_NAME");
      if(name) {
        names.push_back(name.value());
      }
    }

    return names;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace wrapper
} // namespace rdbms
} // namespace cta
