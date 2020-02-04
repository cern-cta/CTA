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

#include "rdbms/ConnPool.hpp"
#include "rdbms/AutocommitMode.hpp"
#include "StatisticsSaveCmd.hpp"
#include "StatisticsSchema.hpp"
#include "catalogue/SchemaChecker.hpp"
#include "MysqlStatisticsSchema.hpp"


namespace cta {
namespace statistics {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
StatisticsSaveCmd::StatisticsSaveCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream):
CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
StatisticsSaveCmd::~StatisticsSaveCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int StatisticsSaveCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  using namespace cta::catalogue;
  const StatisticsSaveCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const uint64_t maxNbConns = 1;
    
  auto loginCatalogue = rdbms::Login::parseFile(cmdLineArgs.catalogueDbConfigPath);
  rdbms::ConnPool catalogueConnPool(loginCatalogue, maxNbConns);
  auto catalogueConn = catalogueConnPool.getConn();
  
  /*auto loginStatistics = rdbms::Login::parseFile(cmdLineArgs.statisticsDbConfigPath);
  rdbms::ConnPool statisticsConnPool(loginStatistics, maxNbConns);
  auto statisticsConn = statisticsConnPool.getConn();*/

  SchemaChecker::Builder catalogueCheckerBuilder("catalogue",loginCatalogue.dbType,catalogueConn);
  std::unique_ptr<cta::catalogue::SchemaChecker> catalogueChecker;
  catalogueChecker = catalogueCheckerBuilder.build();
  
  SchemaChecker::Status tapeTableStatus = catalogueChecker->checkTableContainsColumns("TAPE",{"VID"});
  SchemaChecker::Status tapeFileTableStatus = catalogueChecker->checkTableContainsColumns("TAPE_FILE",{"ARCHIVE_FILE_ID"});
  SchemaChecker::Status archiveFileTableStatus = catalogueChecker->checkTableContainsColumns("ARCHIVE_FILE",{"SIZE_IN_BYTES"});
  
  if(tapeTableStatus == SchemaChecker::Status::FAILURE || tapeFileTableStatus == SchemaChecker::Status::FAILURE || archiveFileTableStatus == SchemaChecker::Status::FAILURE){
    return EXIT_FAILURE;
  }
 
  /*SchemaChecker::Builder statisticsCheckerBuilder("statistics",loginStatistics.dbType,statisticsConn);
  cta::statistics::MysqlStatisticsSchema mysqlSchema;
  std::unique_ptr<SchemaChecker> statisticsChecker = 
  statisticsCheckerBuilder.useCppSchemaStatementsReader(mysqlSchema)
                          .useSQLiteSchemaComparer()
                          .build();
  statisticsChecker->compareTablesLocatedInSchema();*/
  
  return EXIT_SUCCESS;
}
//------------------------------------------------------------------------------
// tableExists
//------------------------------------------------------------------------------
bool StatisticsSaveCmd::tableExists(const std::string tableName, rdbms::Conn &conn) const {
  const auto names = conn.getTableNames();
  for(const auto &name : names) {
    if(tableName == name) {
      return true;
    }
  }
  return false;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void StatisticsSaveCmd::printUsage(std::ostream &os) {
  StatisticsSaveCmdLineArgs::printUsage(os);
}


} // namespace catalogue
} // namespace cta
