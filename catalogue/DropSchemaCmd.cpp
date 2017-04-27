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

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/DropOracleCatalogueSchema.hpp"
#include "catalogue/DropSchemaCmd.hpp"
#include "catalogue/DropSchemaCmdLineArgs.hpp"
#include "catalogue/DropSqliteCatalogueSchema.hpp"
#include "common/exception/Exception.hpp"
#include "rdbms/ConnFactoryFactory.hpp"

namespace cta {
namespace catalogue {

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
DropSchemaCmd::~DropSchemaCmd() noexcept {
}

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

  // Abort if the schema is already dropped
  {
    auto factory = rdbms::ConnFactoryFactory::create(dbLogin); 
    auto conn = factory->create();

    if(conn->getTableNames().empty()) {
      m_out << "Database contains no tables. Assuming the schema has already been dropped." << std::endl;
      return 0;
    }
  }

  // Abort if the schema is not locked
  {
    const uint64_t nbDbConns = 1;
    auto catalogue = CatalogueFactory::create(dbLogin, nbDbConns);
    if(catalogue->schemaIsLocked()) {
      m_err <<
        "Cannot drop the schema of the catalogue database because the schema is locked.\n"
        "\n"
        "Please see the following command-line tools:\n"
        "    cta-catalogue-schema-lock\n"
        "    cta-catalogue-schema-status\n"
        "    cta-catalogue-schema-unlock" << std::endl;
      return 1;
    }
  }

  if(userConfirmsDropOfSchema(dbLogin)) {
    m_out << "DROPPING the schema of the CTA calalogue database" << std::endl;
    dropCatalogueSchema(dbLogin);
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
  m_out << "You are about to drop the schema of the CTA calalogue database" << std::endl;
  m_out << "    Database name: " << dbLogin.database << std::endl;
  m_out << "Are you sure you want to continue?" << std::endl;

  std::string userResponse;
  while(userResponse != "yes" && userResponse != "no") {
    m_out << "Please type either \"yes\" or \"no\" > ";
    std::getline(m_in, userResponse);
  }

  return userResponse == "yes";
}

//------------------------------------------------------------------------------
// dropCatalogueSchema
//------------------------------------------------------------------------------
void DropSchemaCmd::dropCatalogueSchema(const rdbms::Login &dbLogin) {
  auto factory = rdbms::ConnFactoryFactory::create(dbLogin); 
  auto conn = factory->create();
  try {
    switch(dbLogin.dbType) {
    case rdbms::Login::DBTYPE_IN_MEMORY:
    case rdbms::Login::DBTYPE_SQLITE:
      {
         DropSqliteCatalogueSchema dropSchema;
         conn->executeNonQueries(dropSchema.sql);
      }
      break;
    case rdbms::Login::DBTYPE_ORACLE:
      {
         DropOracleCatalogueSchema dropSchema;
         conn->executeNonQueries(dropSchema.sql);
      }
      break;
    case rdbms::Login::DBTYPE_NONE:
      throw exception::Exception("Cannot delete the schema of  catalogue database without a database type");
    default:
      {
        exception::Exception ex;
        ex.getMessage() << "Unknown database type: value=" << dbLogin.dbType;
        throw ex;
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void DropSchemaCmd::printUsage(std::ostream &os) {
  DropSchemaCmdLineArgs::printUsage(os);
}

} // namespace catalogue
} // namespace cta
