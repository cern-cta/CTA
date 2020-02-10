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
#include "statistics/StatisticsUpdateCmd.hpp"
#include "catalogue/SchemaChecker.hpp"
#include "statistics/StatisticsUpdateCmdLineArgs.hpp"
#include "common/Timer.hpp"
#include "TapeStatisticsUpdater.hpp"

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
    
  auto loginCatalogue = rdbms::Login::parseFile(cmdLineArgs.catalogueDbConfigPath);
  rdbms::ConnPool catalogueConnPool(loginCatalogue, maxNbConns);
  auto catalogueConn = catalogueConnPool.getConn();

  SchemaChecker::Builder catalogueCheckerBuilder("catalogue",loginCatalogue.dbType,catalogueConn);
  std::unique_ptr<cta::catalogue::SchemaChecker> catalogueChecker;
  catalogueChecker = catalogueCheckerBuilder.build();
  
  SchemaChecker::Status tapeTableStatus = catalogueChecker->checkTableContainsColumns("TAPE",{"VID","NB_MASTER_FILES","MASTER_DATA_IN_BYTES"});
  SchemaChecker::Status tapeFileTableStatus = catalogueChecker->checkTableContainsColumns("TAPE_FILE",{"ARCHIVE_FILE_ID"});
  SchemaChecker::Status archiveFileTableStatus = catalogueChecker->checkTableContainsColumns("ARCHIVE_FILE",{"SIZE_IN_BYTES"});
  
  if(tapeTableStatus == SchemaChecker::Status::FAILURE || tapeFileTableStatus == SchemaChecker::Status::FAILURE || archiveFileTableStatus == SchemaChecker::Status::FAILURE){
    return EXIT_FAILURE;
  }
  
  std::unique_ptr<TapeStatisticsUpdater> updater = TapeStatisticsUpdaterFactory::create(loginCatalogue.dbType,catalogueConn);
  std::cout<<"Updating tape statistics in the catalogue..."<<std::endl;
  cta::utils::Timer t;
  updater->updateTapeStatistics();
  std::cout<<"Updated catalogue tape statistics in "<<t.secs()<<", "<<updater->getNbUpdatedTapes()<<" tape(s) have been updated"<<std::endl;  
  
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
