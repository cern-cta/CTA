/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/CreateSchemaCmd.hpp"
#include "catalogue/CreateSchemaCmdLineArgs.hpp"
#include "catalogue/OracleCatalogueSchema.hpp"
#include "catalogue/PostgresCatalogueSchema.hpp"
#include "catalogue/SqliteCatalogueSchema.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"
#include "rdbms/AutocommitMode.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CreateSchemaCmd::CreateSchemaCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream):
CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
CreateSchemaCmd::~CreateSchemaCmd() noexcept {
}

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

  const bool ctaCatalogueTableExists = tableExists("CTA_CATALOGUE", conn);

  if(ctaCatalogueTableExists) {
    std::cerr << "Cannot create the database schema because the CTA_CATALOGUE table already exists" << std::endl;
    return 1;
  }

  switch(login.dbType) {
  case rdbms::Login::DBTYPE_IN_MEMORY:
    std::cerr << "Creating the database schema for an in_memory database is not supported" << std::endl;
    return 1;
  case rdbms::Login::DBTYPE_SQLITE:
    std::cerr << "Creating the database schema for an sqlite database is not supported" << std::endl;
    return 1;
  case rdbms::Login::DBTYPE_POSTGRESQL:
    {
       PostgresCatalogueSchema schema;
       executeNonQueries(conn, schema.sql);
    }
    break;
  case rdbms::Login::DBTYPE_ORACLE:
    {
      OracleCatalogueSchema schema;
      executeNonQueries(conn, schema.sql);
    }
    break;
  case rdbms::Login::DBTYPE_NONE:
    throw exception::Exception("Cannot create a catalogue without a database type");
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
void CreateSchemaCmd::executeNonQueries(rdbms::Conn &conn, const std::string &sqlStmts) {
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

} // namespace catalogue
} // namespace cta
