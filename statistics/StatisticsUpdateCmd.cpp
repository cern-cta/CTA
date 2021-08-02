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

#include "rdbms/ConnPool.hpp"
#include "rdbms/AutocommitMode.hpp"
#include "statistics/StatisticsUpdateCmd.hpp"
#include "catalogue/SchemaChecker.hpp"
#include "statistics/StatisticsUpdateCmdLineArgs.hpp"
#include "common/Timer.hpp"
#include "StatisticsService.hpp"
#include "StatisticsServiceFactory.hpp"
#include <cstdlib>

namespace cta {
namespace statistics {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
StatisticsUpdateCmd::StatisticsUpdateCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream):
CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
StatisticsUpdateCmd::~StatisticsUpdateCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int StatisticsUpdateCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  using namespace cta::catalogue;
  const StatisticsUpdateCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const uint64_t maxNbConns = 1;
    
  //Connect to the database
  auto loginCatalogue = rdbms::Login::parseFile(cmdLineArgs.catalogueDbConfigPath);
  rdbms::ConnPool catalogueConnPool(loginCatalogue, maxNbConns);
  auto catalogueConn = catalogueConnPool.getConn();

  //Checks catalogue database content to perform the update of the TAPE statistics
  SchemaChecker::Builder catalogueCheckerBuilder("catalogue",loginCatalogue.dbType,catalogueConn);
  std::unique_ptr<cta::catalogue::SchemaChecker> catalogueChecker;
  catalogueChecker = catalogueCheckerBuilder.build();
  
  SchemaCheckerResult result = catalogueChecker->checkTableContainsColumns("TAPE",{"VID",
                                                                                              "DIRTY",
                                                                                              "NB_MASTER_FILES",
                                                                                              "MASTER_DATA_IN_BYTES",
                                                                                              "NB_COPY_NB_1",
                                                                                              "COPY_NB_1_IN_BYTES",
                                                                                              "NB_COPY_NB_GT_1",
                                                                                              "COPY_NB_GT_1_IN_BYTES"
                                                                                      });
  result += catalogueChecker->checkTableContainsColumns("TAPE_FILE",{"VID","ARCHIVE_FILE_ID","FSEQ","COPY_NB"});
  result += catalogueChecker->checkTableContainsColumns("ARCHIVE_FILE",{"ARCHIVE_FILE_ID","SIZE_IN_BYTES"});
  
  if(result.getStatus() == SchemaCheckerResult::FAILED){
    result.displayErrors(std::cerr);
    return EXIT_FAILURE;
  }
  
  //Create the Statistics service
  std::unique_ptr<StatisticsService> service = StatisticsServiceFactory::create(catalogueConn,loginCatalogue.dbType);
  //Update TAPE statistics
  std::cout<<"Updating tape statistics in the catalogue..."<<std::endl;
  cta::utils::Timer t;
  service->updateStatisticsPerTape();
  std::cout<<"Updated catalogue tape statistics in "<<t.secs()<<" seconds, "<<service->getNbUpdatedTapes()<<" tape(s) have been updated"<<std::endl; 
  
  return EXIT_SUCCESS;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void StatisticsUpdateCmd::printUsage(std::ostream &os) {
  StatisticsUpdateCmdLineArgs::printUsage(os);
}


} // namespace statistics
} // namespace cta
