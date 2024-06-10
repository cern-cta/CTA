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

#include "catalogue/VerifySchemaCmd.hpp"
#include "catalogue/VerifySchemaCmdLineArgs.hpp"
#include "catalogue/OracleCatalogueSchema.hpp"
#include "catalogue/PostgresCatalogueSchema.hpp"
#include "catalogue/SqliteCatalogueSchema.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "catalogue/SQLiteSchemaInserter.hpp"
#include "common/exception/Exception.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/AutocommitMode.hpp"
#include "common/log/DummyLogger.hpp"
#include "Catalogue.hpp"
#include "SchemaChecker.hpp"
#include "SQLiteSchemaComparer.hpp"

#include <algorithm>
#include <map>

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
VerifySchemaCmd::VerifySchemaCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream):
CmdLineTool(inStream, outStream, errStream) {
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

  auto login = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t maxNbConns = 1;
  rdbms::ConnPool connPool(login, maxNbConns);
  auto conn = connPool.getConn();

  if(!tableExists("CTA_CATALOGUE", conn)) {
    std::cerr << "Cannot verify the database schema because the CTA_CATALOGUE table does not exist" << std::endl;
    return 1;
  }

  SchemaChecker::Builder schemaCheckerBuilder("catalogue",login.dbType,conn);
  std::unique_ptr<SchemaChecker> schemaChecker(schemaCheckerBuilder
                        .useMapStatementsReader()
                        .useSQLiteSchemaComparer()
                        .build());

  SchemaCheckerResult result = schemaChecker->displayingCompareSchema(std::cout,std::cerr);
  result += schemaChecker->warnParallelTables();
  result += schemaChecker->warnSchemaUpgrading();
  result += schemaChecker->warnProcedures();
  result += schemaChecker->warnSynonyms();
  result += schemaChecker->warnTypes();
  result += schemaChecker->warnErrorLoggingTables();
  result += schemaChecker->warnMissingIndexes();
  result.displayWarnings(std::cout);
  if(result.getStatus() == SchemaCheckerResult::Status::FAILED
    && isUpgrading(&conn)) {
    return 2;
  }
  if(result.getStatus() == SchemaCheckerResult::Status::FAILED){
    return 1;
  }
  return 0;
}

//------------------------------------------------------------------------------
// tableExists
//------------------------------------------------------------------------------
bool VerifySchemaCmd::tableExists(const std::string& tableName, const rdbms::Conn &conn) const {
  const auto names = conn.getTableNames();
  return std::any_of(names.begin(), names.end(), [&](const auto& name) {
    return tableName == name;
  });
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void VerifySchemaCmd::printUsage(std::ostream &os) {
  VerifySchemaCmdLineArgs::printUsage(os);
}

bool VerifySchemaCmd::isUpgrading(rdbms::Conn *conn) const {
  const char* const sql = R"SQL(
    SELECT 
      CTA_CATALOGUE.STATUS AS STATUS 
    FROM 
      CTA_CATALOGUE
  )SQL";

  auto stmt = conn->createStmt(sql);
  auto rset = stmt.executeQuery();

  if(!rset.next()) {
    throw exception::Exception("CTA_CATALOGUE does not contain any STATUS");
  }
  return rset.columnString("STATUS") == "UPGRADING";
}

} // namespace cta::catalogue
