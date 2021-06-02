/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#include "rdbms/ConnPool.hpp"
#include "rdbms/AutocommitMode.hpp"
#include "StatisticsSaveCmd.hpp"
#include "catalogue/SchemaChecker.hpp"
#include "StatisticsSchemaFactory.hpp"
#include "StatisticsService.hpp"
#include "StatisticsServiceFactory.hpp"
#include "common/utils/utils.hpp"

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

  bool isJsonOutput = cmdLineArgs.jsonOutput;
  
  const uint64_t maxNbConns = 1;
      
  std::unique_ptr<StatisticsService> statisticsDbService;
  std::unique_ptr<StatisticsService> statisticsStdoutJsonService = StatisticsServiceFactory::create(std::cout);
  
  std::unique_ptr<rdbms::ConnPool> statisticsConnPool;
  std::unique_ptr<rdbms::Conn> statisticsConn;
  
  std::unique_ptr<Statistics> computedStatistics = nullptr;
  
  if(!cmdLineArgs.buildDatabase && !cmdLineArgs.dropDatabase){
    //Compute the statistics from the Catalogue
    //Connect to the catalogue database
    auto loginCatalogue = rdbms::Login::parseFile(cmdLineArgs.catalogueDbConfigPath);
    rdbms::ConnPool catalogueConnPool(loginCatalogue, maxNbConns);
    auto catalogueConn = catalogueConnPool.getConn();

    checkCatalogueSchema(catalogueConn,loginCatalogue.dbType);
    
    //Compute the statistics
    std::unique_ptr<StatisticsService> catalogueStatisticsService = StatisticsServiceFactory::create(catalogueConn,loginCatalogue.dbType);
    computedStatistics = catalogueStatisticsService->getStatistics();
  }
  try {
    if(!isJsonOutput){

      auto loginStatistics = rdbms::Login::parseFile(cmdLineArgs.statisticsDbConfigPath);
      statisticsConnPool = cta::make_unique<rdbms::ConnPool>(loginStatistics, maxNbConns);
      statisticsConn = cta::make_unique<rdbms::Conn>(statisticsConnPool->getConn());

      //Get the statistics schema so that we can create, drop and check the presence of the database
      auto statisticsSchema = StatisticsSchemaFactory::create(loginStatistics.dbType);

      if(cmdLineArgs.buildDatabase){
        //Build the database
        buildStatisticsDatabase(*statisticsConn,*statisticsSchema);
        return EXIT_SUCCESS;
      }

      if(cmdLineArgs.dropDatabase){
        //drop the database
        if(userConfirmDropStatisticsSchemaFromDb(loginStatistics)){
          dropStatisticsDatabase(*statisticsConn,*statisticsSchema);
        }
        return EXIT_SUCCESS;
      }
      
      //Here, we want to store the statistics in the Statistics database
      checkStatisticsSchema(*statisticsConn,loginStatistics.dbType);
      
      //Create the statistics service with the statistics database connection
      statisticsDbService = StatisticsServiceFactory::create(*statisticsConn,loginStatistics.dbType);
    } else if (isJsonOutput){
      //Create the statistics service with the JSON output connection
      statisticsDbService = StatisticsServiceFactory::create(std::cout);
    } else {
      //We should normally not be there
      throw cta::exception::Exception("No --json option provided and no statistics database configuration file provided.");
    }
  
    //Insert the statistics to the statistics database
    if(!isJsonOutput){
      statisticsDbService->saveStatistics(*computedStatistics);
    }
    //In any case, output the statistics in json format
    statisticsStdoutJsonService->saveStatistics(*computedStatistics);
  } catch (cta::exception::Exception &ex) {
    std::cerr << ex.getMessageValue() << std::endl;
    if(computedStatistics != nullptr){
      std::unique_ptr<StatisticsService> statisticsStderrJsonService = StatisticsServiceFactory::create(std::cerr);
      std::cerr << "StatisticsJson:" << std::endl;
      statisticsStderrJsonService->saveStatistics(*computedStatistics);
      std::cerr << std::endl;
    }
    return EXIT_FAILURE;
  }
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
  if(cmdLineArgs.jsonOutput && !statisticsDbConfigPathEmpty){
    throw cta::exception::Exception("The statistics database connection file should not be provided when --json flag is set.");
  }
  if((!cmdLineArgs.buildDatabase && !cmdLineArgs.dropDatabase && !cmdLineArgs.jsonOutput) && (catalogueDbConfigPathEmpty || statisticsDbConfigPathEmpty)){
    throw cta::exception::Exception("You should provide the catalogue database and the statistics database connection files.");
  }
  if((!cmdLineArgs.buildDatabase && !cmdLineArgs.dropDatabase && cmdLineArgs.jsonOutput) && catalogueDbConfigPathEmpty){
    throw cta::exception::Exception("The catalogue database connection file should be provided when --json flag is set.");
  }
}

void StatisticsSaveCmd::buildStatisticsDatabase(cta::rdbms::Conn& statisticsDatabaseConn, const StatisticsSchema& statisticsSchema) {
  try {
    std::string::size_type searchPos = 0;
    std::string::size_type findResult = std::string::npos;
    std::string sqlStmts = statisticsSchema.sql;

    while(std::string::npos != (findResult = sqlStmts.find(';', searchPos))) {
      // Calculate the length of the current statement without the trailing ';'
      const std::string::size_type stmtLen = findResult - searchPos;
      const std::string sqlStmt = utils::trimString(sqlStmts.substr(searchPos, stmtLen));
      searchPos = findResult + 1;

      if(0 < sqlStmt.size()) { // Ignore empty statements
        statisticsDatabaseConn.executeNonQuery(sqlStmt);
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
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

void StatisticsSaveCmd::checkCatalogueSchema(cta::rdbms::Conn& catalogueConn, cta::rdbms::Login::DbType dbType) {
  using namespace cta::catalogue;
  //Check that the catalogue contains the table TAPE with the columns we need
  SchemaChecker::Builder catalogueCheckerBuilder("catalogue",dbType,catalogueConn);
  std::unique_ptr<cta::catalogue::SchemaChecker> catalogueChecker = catalogueCheckerBuilder.build();

  SchemaCheckerResult result = 
    catalogueChecker->checkTableContainsColumns("TAPE",{
                                                        "NB_MASTER_FILES",
                                                        "MASTER_DATA_IN_BYTES",
                                                        "NB_COPY_NB_1",
                                                        "COPY_NB_1_IN_BYTES",
                                                        "NB_COPY_NB_GT_1",
                                                        "COPY_NB_GT_1_IN_BYTES",
                                                        "DIRTY"
                                                      }
                                                );
  result += catalogueChecker->checkTableContainsColumns("TAPE_POOL",{"VIRTUAL_ORGANIZATION_ID"});

  if(result.getStatus() == SchemaCheckerResult::FAILED){
    result.displayErrors(std::cerr);
    throw cta::exception::Exception("Catalogue schema checking failed.");
  }
}


void StatisticsSaveCmd::checkStatisticsSchema(cta::rdbms::Conn& statisticsDatabaseConn, cta::rdbms::Login::DbType dbType) {
  using namespace cta::catalogue;
  auto statisticsSchema = StatisticsSchemaFactory::create(dbType);
   //Check that the statistics schema tables are in the statistics database
  SchemaChecker::Builder statisticsCheckerBuilder("statistics",dbType,statisticsDatabaseConn);
  std::unique_ptr<SchemaChecker> statisticsChecker = 
  statisticsCheckerBuilder.useCppSchemaStatementsReader(*statisticsSchema)
                          .useSQLiteSchemaComparer()
                          .build();
  SchemaCheckerResult compareTablesStatus = statisticsChecker->compareTablesLocatedInSchema();
  if(compareTablesStatus.getStatus() == SchemaCheckerResult::FAILED){
    compareTablesStatus.displayErrors(std::cerr);
    throw cta::exception::Exception("Statistics schema checking failed.");
  }
}



} // namespace statistics
} // namespace cta
