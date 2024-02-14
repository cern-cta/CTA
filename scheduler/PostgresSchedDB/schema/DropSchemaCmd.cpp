/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "scheduler/PostgresSchedDB/schema/DropSchemaCmd.hpp"
#include "scheduler/PostgresSchedDB/schema/DropSchemaCmdLineArgs.hpp"
#include "common/exception/Exception.hpp"
#include "rdbms/ConnPool.hpp"

#include <algorithm>

namespace cta::postgresscheddb {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DropSchemaCmd::DropSchemaCmd(
  std::istream &inStream,
  std::ostream &outStream,
  std::ostream &errStream):
  CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
DropSchemaCmd::~DropSchemaCmd() = default;

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int DropSchemaCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const DropSchemaCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const rdbms::Login dbLogin = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t maxNbConns = 1;
  rdbms::ConnPool connPool(dbLogin, maxNbConns);
  auto conn = connPool.getConn();

  // Abort if the schema is already dropped
  if(conn.getTableNames().empty() && conn.getSequenceNames().empty()) {
    m_out << "Database contains no tables and no sequences." << std::endl <<
      "Assuming the schema has already been dropped." << std::endl;
    return 0;
  }

  if (isProductionSet(conn)) {
    throw cta::exception::Exception("Cannot drop a production database. If you still wish to proceed then please modify the database manually to remove its production status before trying again.");
  }

  if(userConfirmsDropOfSchema(dbLogin)) {
    m_out << "DROPPING the schema of the CTA calalogue database" << std::endl;
    dropSchedulerSchema(dbLogin.dbType, conn);
  } else {
    m_out << "Aborting" << std::endl;
  }

  return 0;
}

//------------------------------------------------------------------------------
// userConfirmsDropOfSchema
//------------------------------------------------------------------------------
bool DropSchemaCmd::userConfirmsDropOfSchema(const rdbms::Login &dbLogin) {
  m_out << "WARNING" << std::endl;

  m_out << "You are about to drop ALL tables and sequences from the following database:" << std::endl;
  m_out << "    Database name: " << dbLogin.database << std::endl;
  m_out << "Are you sure you want to continue?" << std::endl;

  std::string userResponse;
  while(userResponse != "yes" && userResponse != "no") {
    m_out << R"#(Please type either "yes" or "no" > )#";
    std::getline(m_in, userResponse);
  }

  return userResponse == "yes";
}

//------------------------------------------------------------------------------
// dropSchedulerSchema
//------------------------------------------------------------------------------
void DropSchemaCmd::dropSchedulerSchema(const rdbms::Login::DbType &dbType, rdbms::Conn &conn) {
  try {
    switch (dbType) {
    case rdbms::Login::DBTYPE_IN_MEMORY:
      throw exception::Exception("Dropping the schema of an in_memory database is not supported");
    case rdbms::Login::DBTYPE_SQLITE:
      throw exception::Exception("Dropping the schema of an sqlite database is not supported");
    case rdbms::Login::DBTYPE_NONE:
      throw exception::Exception("Cannot delete the schema of scheduler database without a database type");
    default:
      dropDatabaseSequences(conn);
      dropDatabaseTables(conn);
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// dropDatabaseTables
//------------------------------------------------------------------------------
void DropSchemaCmd::dropDatabaseTables(rdbms::Conn &conn) {
  try {
    bool droppedAtLeastOneTable = true;
    while (droppedAtLeastOneTable) {
      droppedAtLeastOneTable = false;
      auto tables = conn.getTableNames();
      tables.remove("CTA_SCHEDULER");  // Remove CTA_SCHEDULER to drop it at the end
      for (const auto &table : tables) {
        conn.executeNonQuery(std::string("DROP TABLE ") + table);
        m_out << "Dropped table " << table << std::endl;
        droppedAtLeastOneTable = true;
      }
    }

    // Drop CTA_SCHEDULER table
    auto tables = conn.getTableNames();
    if (tables.size() != 1) {
      throw exception::Exception("Failed to delete all tables, except CTA_SCHEDULER.");
    }
    conn.executeNonQuery("DROP TABLE CTA_SCHEDULER");
    m_out << "Dropped table CTA_SCHEDULER" << std::endl;

    tables = conn.getTableNames();
    if (!tables.empty()) {
      throw exception::Exception("Failed to delete all tables.  Maybe there is a circular dependency.");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// dropDatabaseSequences
//------------------------------------------------------------------------------
void DropSchemaCmd::dropDatabaseSequences(rdbms::Conn &conn) {
  try {
    std::list<std::string> sequences = conn.getSequenceNames();
    for(const auto& sequence : sequences) {
      conn.executeNonQuery(std::string("DROP SEQUENCE ") + sequence);
      m_out << "Dropped sequence " << sequence << std::endl;
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// isProductionSet
//------------------------------------------------------------------------------
bool DropSchemaCmd::isProductionSet(cta::rdbms::Conn & conn){
  const char * const sql = "SELECT CTA_SCHEDULER.IS_PRODUCTION AS IS_PRODUCTION FROM CTA_SCHEDULER";
  try {
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    if(rset.next()){
      return rset.columnBool("IS_PRODUCTION");
    } else {
      return false;  // The table is empty
    }
  } catch(exception::Exception & ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void DropSchemaCmd::printUsage(std::ostream &os) {
  DropSchemaCmdLineArgs::printUsage(os);
}

} // namespace cta::postgresscheddb
