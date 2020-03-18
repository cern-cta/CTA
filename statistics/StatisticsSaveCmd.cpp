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
#include "catalogue/SchemaChecker.hpp"
#include "StatisticsSchemaFactory.hpp"
#include "StatisticsGetter.hpp"
#include <algorithm>

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
  
  verifyCmdLineArgs(cmdLineArgs);

  const uint64_t maxNbConns = 1;
  
  auto loginStatistics = rdbms::Login::parseFile(cmdLineArgs.statisticsDbConfigPath);
  rdbms::ConnPool statisticsConnPool(loginStatistics, maxNbConns);
  auto statisticsConn = statisticsConnPool.getConn();
  
  auto statisticsSchema = StatisticsSchemaFactory::create(loginStatistics.dbType);
  
  if(cmdLineArgs.buildDatabase){
    //Build the database
    buildStatisticsDatabase(statisticsConn,*statisticsSchema);
    return EXIT_SUCCESS;
  }
  
  if(cmdLineArgs.dropDatabase){
    //drop the database
    if(userConfirmDropStatisticsSchemaFromDb(loginStatistics)){
      dropStatisticsDatabase(statisticsConn,*statisticsSchema);
    }
    return EXIT_SUCCESS;
  }
    
  //Save the CTA statistics
  
  //Check the content of the CTA catalogue
  auto loginCatalogue = rdbms::Login::parseFile(cmdLineArgs.catalogueDbConfigPath);
  rdbms::ConnPool catalogueConnPool(loginCatalogue, maxNbConns);
  auto catalogueConn = catalogueConnPool.getConn();

  SchemaChecker::Builder catalogueCheckerBuilder("catalogue",loginCatalogue.dbType,catalogueConn);
  std::unique_ptr<cta::catalogue::SchemaChecker> catalogueChecker = catalogueCheckerBuilder.build();
  
  SchemaChecker::Status tapeTableStatus = catalogueChecker->checkTableContainsColumns("TAPE",{"VID","NB_MASTER_FILES","MASTER_DATA_IN_BYTES","DIRTY"});
  
  if(tapeTableStatus == SchemaChecker::Status::FAILURE){
    return EXIT_FAILURE;
  }
 
  //Check that the schema tables are in the statistics database
  SchemaChecker::Builder statisticsCheckerBuilder("statistics",loginStatistics.dbType,statisticsConn);
  std::unique_ptr<SchemaChecker> statisticsChecker = 
  statisticsCheckerBuilder.useCppSchemaStatementsReader(*statisticsSchema)
                          .useSQLiteSchemaComparer()
                          .build();
  statisticsChecker->compareTablesLocatedInSchema();
  
  //Compute the statistics
  std::unique_ptr<StatisticsGetter> statisticsGetter = StatisticsGetterFactory::create(catalogueConn,loginCatalogue.dbType);
  std::unique_ptr<Statistics> statistics = statisticsGetter->getAllStatistics();
  std::cout << *statistics << std::endl;
  //Insert them into the statistics database
  return EXIT_SUCCESS;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void StatisticsSaveCmd::printUsage(std::ostream &os) {
  StatisticsSaveCmdLineArgs::printUsage(os);
}

void StatisticsSaveCmd::verifyCmdLineArgs(const StatisticsSaveCmdLineArgs& cmdLineArgs) const {
  bool catalogueDbConfigPathEmpty = cmdLineArgs.catalogueDbConfigPath.empty();
  bool statisticsDbConfigPathEmpty = cmdLineArgs.statisticsDbConfigPath.empty();
  
  if(cmdLineArgs.buildDatabase && cmdLineArgs.dropDatabase){
    throw cta::exception::Exception("--build and --drop are mutually exclusive.");
  }
  if(cmdLineArgs.buildDatabase && !catalogueDbConfigPathEmpty){
    throw cta::exception::Exception("The catalogue database configuration file should not be provided when --build flag is set.");
  }
  if(cmdLineArgs.dropDatabase && !catalogueDbConfigPathEmpty){
    throw cta::exception::Exception("The catalogue database configuration file should not be provided when --drop flag is set.");
  }
  if(statisticsDbConfigPathEmpty && (cmdLineArgs.buildDatabase || cmdLineArgs.dropDatabase)){
    throw cta::exception::Exception("The statistics database configuration file should be provided.");
  }
  if((!cmdLineArgs.buildDatabase && !cmdLineArgs.dropDatabase) && (catalogueDbConfigPathEmpty || statisticsDbConfigPathEmpty)){
    throw cta::exception::Exception("You should provide the catalogue database and the statistics database connection files.");
  } 
}

void StatisticsSaveCmd::buildStatisticsDatabase(cta::rdbms::Conn& statisticsDatabaseConn, const StatisticsSchema& statisticsSchema) {
  statisticsDatabaseConn.executeNonQuery(statisticsSchema.sql);
}

bool StatisticsSaveCmd::userConfirmDropStatisticsSchemaFromDb(const rdbms::Login &dbLogin) {
  m_out << "WARNING" << std::endl;
  m_out << "You are about to drop the schema of the statistics database" << std::endl;
  m_out << "    Database name: " << dbLogin.database << std::endl;
  m_out << "Are you sure you want to continue?" << std::endl;

  std::string userResponse;
  while(userResponse != "yes" && userResponse != "no") {
    m_out << "Please type either \"yes\" or \"no\" > ";
    std::getline(m_in, userResponse);
  }
  return userResponse == "yes";
}

void StatisticsSaveCmd::dropStatisticsDatabase(cta::rdbms::Conn& statisticsDatabaseConn,const StatisticsSchema& statisticsSchema) {
  try {
    std::list<std::string> tablesInDb = statisticsDatabaseConn.getTableNames();
    std::list<std::string> statisticsTables = statisticsSchema.getSchemaTableNames();
    for(auto & tableToDrop: statisticsTables){
      const bool tableToDropIsInDb = (tablesInDb.end() != std::find(tablesInDb.begin(), tablesInDb.end(), tableToDrop));
      if(tableToDropIsInDb) {
        statisticsDatabaseConn.executeNonQuery(std::string("DROP TABLE ") + tableToDrop);
        m_out << "Dropped table " << tableToDrop << std::endl;
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace statistics
} // namespace cta
