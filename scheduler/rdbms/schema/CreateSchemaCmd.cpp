/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/schema/CreateSchemaCmd.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/AutocommitMode.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"
#include "scheduler/rdbms/schema/CreateSchemaCmdLineArgs.hpp"
#include "scheduler/rdbms/schema/PostgresSchedulerSchema.hpp"

#include <algorithm>

namespace cta::schedulerdb {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CreateSchemaCmd::CreateSchemaCmd(std::istream& inStream, std::ostream& outStream, std::ostream& errStream)
    : CmdLineTool(inStream, outStream, errStream) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
CreateSchemaCmd::~CreateSchemaCmd() = default;

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int CreateSchemaCmd::exceptionThrowingMain(const int argc, char* const* const argv) {
  const CreateSchemaCmdLineArgs cmdLineArgs(argc, argv);

  if (cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const auto login = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t maxNbConns = 1;
  rdbms::ConnPool connPool(login, maxNbConns);
  auto conn = connPool.getConn();

  if (tableExists("CTA_SCHEDULER", conn)) {
    std::cerr << "Cannot create the database schema because the CTA_SCHEDULER table already exists" << std::endl;
    return 1;
  }

  switch (login.dbType) {
    case rdbms::Login::DBTYPE_POSTGRESQL: {
      if (cmdLineArgs.schedulerdbVersion) {
        throw exception::NotImplementedException("Create a schedulerdb of given version");
      } else {
        PostgresSchedulerSchema schema(login.username, login.dbNamespace);
        executeNonQueries(conn, schema.sql);
        conn.commit();
      }
    } break;
    case rdbms::Login::DBTYPE_NONE:
      throw exception::Exception("Cannot create a schedulerdb without a database type");
    default: {
      exception::Exception ex;
      ex.getMessage() << "Unknown database type: value=" << login.dbType;
      throw ex;
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// tableExists
//------------------------------------------------------------------------------
bool CreateSchemaCmd::tableExists(const std::string& tableName, rdbms::Conn& conn) const {
  const auto names = conn.getTableNames();
  return std::any_of(std::begin(names), std::end(names), [&tableName](const std::string& str) {
    return str == tableName;
  });
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void CreateSchemaCmd::printUsage(std::ostream& os) {
  CreateSchemaCmdLineArgs::printUsage(os);
}

//------------------------------------------------------------------------------
// executeNonQueries
//------------------------------------------------------------------------------
void CreateSchemaCmd::executeNonQueries(rdbms::Conn& conn, std::string_view sqlStmts) const {
  std::string::size_type searchPos = 0;
  std::string::size_type findResult = std::string::npos;

  while (std::string::npos != (findResult = sqlStmts.find(';', searchPos))) {
    // Calculate the length of the current statement without the trailing ';'
    const std::string::size_type stmtLen = findResult - searchPos;
    const std::string sqlStmt = utils::trimString(sqlStmts.substr(searchPos, stmtLen));
    searchPos = findResult + 1;

    if (0 < sqlStmt.size()) {  // Ignore empty statements
      conn.executeNonQuery(sqlStmt);
    }
  }
}

}  // namespace cta::schedulerdb
