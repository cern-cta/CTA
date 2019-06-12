/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
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

#include "catalogue/VerifySchemaCmd.hpp"
#include "catalogue/VerifySchemaCmdLineArgs.hpp"
#include "catalogue/MysqlCatalogueSchema.hpp"
#include "catalogue/OracleCatalogueSchema.hpp"
#include "catalogue/PostgresCatalogueSchema.hpp"
#include "catalogue/SqliteCatalogueSchema.hpp"
#include "common/exception/Exception.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"
#include "rdbms/AutocommitMode.hpp"

#include <algorithm>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
VerifySchemaCmd::VerifySchemaCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream):
CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
VerifySchemaCmd::~VerifySchemaCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int VerifySchemaCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const VerifySchemaCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const auto login = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t maxNbConns = 1;
  rdbms::ConnPool connPool(login, maxNbConns);
  auto conn = connPool.getConn();;

  const bool ctaCatalogueTableExists = tableExists("CTA_CATALOGUE", conn);

  if(!ctaCatalogueTableExists) {
    std::cerr << "Cannot verify the database schema because the CTA_CATALOGUE table does not exists" << std::endl;
    return 1;
  }

  switch(login.dbType) {
  case rdbms::Login::DBTYPE_IN_MEMORY:
  case rdbms::Login::DBTYPE_SQLITE:
    {
      // TODO
      SqliteCatalogueSchema schema;
      std::cerr << "Checking table names..." << std::endl;
      const auto schemaTableNames = schema.getSchemaTableNames();
      const auto dbTableNames = conn.getTableNames();
      std::cerr << "error code: "<< static_cast<int>(verifyTableNames(schemaTableNames, dbTableNames)) << std::endl;

      std::cerr << "Checking index names..." << std::endl;
      const auto schemaIndexNames = schema.getSchemaIndexNames();
      const auto dbIndexNames = conn.getIndexNames();
      std::cerr << "error code: "<< static_cast<int>(verifyIndexNames(schemaIndexNames, dbIndexNames)) << std::endl;
    }
    break;
  case rdbms::Login::DBTYPE_POSTGRESQL:
    {
       // TODO
       // PostgresCatalogueSchema schema;
       // conn.executeNonQueries(schema.sql);
    }
    break;
  case rdbms::Login::DBTYPE_MYSQL:
    {
       // TODO
       //MysqlCatalogueSchema schema;
       //conn.executeNonQueries(schema.sql);

       // execute triggers
       //auto triggers = schema.triggers();
       //for (auto trigger: triggers) {
       //  conn.executeNonQuery(trigger);
       //}
    }
    break;
  case rdbms::Login::DBTYPE_ORACLE:
    {
      // TODO
      OracleCatalogueSchema schema;
      std::cerr << "Checking table names..." << std::endl;
      const auto schemaTableNames = schema.getSchemaTableNames();
      const auto dbTableNames = conn.getTableNames();
      std::cerr << "error code: "<< static_cast<int>(verifyTableNames(schemaTableNames, dbTableNames)) << std::endl;

      std::cerr << "Checking index names..." << std::endl;
      const auto schemaIndexNames = schema.getSchemaIndexNames();
      const auto dbIndexNames = conn.getIndexNames();
      std::cerr << "error code: "<< static_cast<int>(verifyIndexNames(schemaIndexNames, dbIndexNames)) << std::endl;
    }
    break;
  case rdbms::Login::DBTYPE_NONE:
    throw exception::Exception("Cannot verify a catalogue without a database type");
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
bool VerifySchemaCmd::tableExists(const std::string tableName, rdbms::Conn &conn) const {
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
void VerifySchemaCmd::printUsage(std::ostream &os) {
  VerifySchemaCmdLineArgs::printUsage(os);
}

//------------------------------------------------------------------------------
// verifyTableNames
//------------------------------------------------------------------------------
VerifySchemaCmd::VerifyStatus VerifySchemaCmd::verifyTableNames(const std::list<std::string> &schemaTableNames, 
  const std::list<std::string> &dbTableNames) const {
  VerifyStatus status = VerifyStatus::UNKNOWN;
  // check for schema tables
  for(auto &tableName : schemaTableNames) {
    const bool schemaTableIsNotInDb = dbTableNames.end() == std::find(dbTableNames.begin(), dbTableNames.end(), tableName);
    if (schemaTableIsNotInDb) {
      std::cerr << "  ERROR: the schema table " << tableName << " not found in the DB" << std::endl;
      status = VerifyStatus::ERROR;
    }
  }
  // check for extra tables in DB
  for(auto &tableName : dbTableNames) {
    const bool dbTableIsNotInSchema = schemaTableNames.end() == std::find(schemaTableNames.begin(), schemaTableNames.end(), tableName);
    if (dbTableIsNotInSchema) {
      std::cerr << "  WARNING: the database table " << tableName << " not found in the schema" << std::endl;
      if ( VerifyStatus::ERROR != status) {
        status = VerifyStatus::WARNING;
      }
    }
  }
  return status;
}
  
//------------------------------------------------------------------------------
// verifyIndexNames
//------------------------------------------------------------------------------
VerifySchemaCmd::VerifyStatus VerifySchemaCmd::verifyIndexNames(const std::list<std::string> &schemaIndexNames, 
  const std::list<std::string> &dbIndexNames) const {
  VerifyStatus status = VerifyStatus::UNKNOWN;
  // check for schema tables
  for(auto &indexName : schemaIndexNames) {
    const bool schemaIndexIsNotInDb = dbIndexNames.end() == std::find(dbIndexNames.begin(), dbIndexNames.end(), indexName);
    if (schemaIndexIsNotInDb) {
      std::cerr << "  ERROR: the schema index " << indexName << " not found in the DB" << std::endl;
      status = VerifyStatus::ERROR;
    }
  }
  // check for extra tables in DB
  for(auto &indexName : dbIndexNames) {
    const bool dbIndexIsNotInSchema = schemaIndexNames.end() == std::find(schemaIndexNames.begin(), schemaIndexNames.end(), indexName);
    if (dbIndexIsNotInSchema) {
      std::cerr << "  WARNING: the database index " << indexName << " not found in the schema" << std::endl;
      if ( VerifyStatus::ERROR != status) {
        status = VerifyStatus::WARNING;
      }
    }
  }
  return status;
}

} // namespace catalogue
} // namespace cta
