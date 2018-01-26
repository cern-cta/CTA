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

#include "catalogue/CreateSchemaCmd.hpp"
#include "catalogue/CreateSchemaCmdLineArgs.hpp"
#include "catalogue/OracleCatalogueSchema.hpp"
#include "catalogue/SqliteCatalogueSchema.hpp"
#include "common/exception/Exception.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"

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
  case rdbms::Login::DBTYPE_SQLITE:
    {
       SqliteCatalogueSchema schema;
       conn.executeNonQueries(schema.sql);
    }
    break;
  case rdbms::Login::DBTYPE_ORACLE:
    {
      OracleCatalogueSchema schema;
      conn.executeNonQueries(schema.sql);
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

} // namespace catalogue
} // namespace cta
