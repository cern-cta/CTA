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

#include "scheduler/PostgresSchedDB/schema/CreateSchemaCmd.hpp"
#include "scheduler/PostgresSchedDB/schema/CreateSchemaCmdLineArgs.hpp"
#include "scheduler/PostgresSchedDB/schema/PostgresSchedulerSchema.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"
#include "rdbms/AutocommitMode.hpp"

namespace cta::postgresscheddb {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CreateSchemaCmd::CreateSchemaCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream):
CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
CreateSchemaCmd::~CreateSchemaCmd() = default;

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int CreateSchemaCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const CreateSchemaCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const auto login = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t maxNbConns = 1;
  rdbms::ConnPool connPool(login, maxNbConns);
  auto conn = connPool.getConn();;

  const bool ctaSchedulerTableExists = tableExists("CTA_SCHEDULER", conn);

  if(ctaSchedulerTableExists) {
    std::cerr << "Cannot create the database schema because the CTA_SCHEDULER table already exists" << std::endl;
    return 1;
  }

  switch(login.dbType) {
  case rdbms::Login::DBTYPE_POSTGRESQL:
    {
      if (cmdLineArgs.schedulerdbVersion) {
        throw exception::Exception("Not implemented: create a schedulerdb of given version");
      } else {
        PostgresSchedulerSchema schema;
        executeNonQueries(conn, schema.sql);
      }
    }
    break;
  case rdbms::Login::DBTYPE_NONE:
    throw exception::Exception("Cannot create a schedulerdb without a database type");
  default:
    {
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
bool CreateSchemaCmd::tableExists(const std::string tableName, rdbms::Conn &conn) const {
  const auto names = conn.getTableNames();
  for(auto &name : names) {
    if(tableName == name) {
      return true;
    }
  }
  return false;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void CreateSchemaCmd::printUsage(std::ostream &os) {
  CreateSchemaCmdLineArgs::printUsage(os);
}

//------------------------------------------------------------------------------
// executeNonQueries
//------------------------------------------------------------------------------
void CreateSchemaCmd::executeNonQueries(rdbms::Conn &conn, const std::string &sqlStmts) const {
  try {
    std::string::size_type searchPos = 0;
    std::string::size_type findResult = std::string::npos;

    while(std::string::npos != (findResult = sqlStmts.find(';', searchPos))) {
      // Calculate the length of the current statement without the trailing ';'
      const std::string::size_type stmtLen = findResult - searchPos;
      const std::string sqlStmt = utils::trimString(sqlStmts.substr(searchPos, stmtLen));
      searchPos = findResult + 1;

      if(0 < sqlStmt.size()) { // Ignore empty statements
        conn.executeNonQuery(sqlStmt);
      }
    }

  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace cta::postgresscheddb
