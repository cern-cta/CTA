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

#include <memory>

#include "catalogue/SchemaChecker.hpp"
#include "rdbms/ConnPool.hpp"
#include "StatisticsSaveCmd.hpp"
#include "StatisticsService.hpp"
#include "StatisticsServiceFactory.hpp"

namespace cta {
namespace statistics {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
StatisticsSaveCmd::StatisticsSaveCmd(std::istream& inStream, std::ostream& outStream, std::ostream& errStream) :
CmdLineTool(inStream, outStream, errStream) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
StatisticsSaveCmd::~StatisticsSaveCmd() noexcept {}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int StatisticsSaveCmd::exceptionThrowingMain(const int argc, char* const* const argv) {
  const StatisticsSaveCmdLineArgs cmdLineArgs(argc, argv);

  if (cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  verifyCmdLineArgs(cmdLineArgs);

  const uint64_t maxNbConns = 1;

  std::unique_ptr<StatisticsService> statisticsDbService;
  std::unique_ptr<StatisticsService> statisticsStdoutJsonService = StatisticsServiceFactory::create(std::cout);

  // Compute the statistics from the Catalogue
  // Connect to the catalogue database
  auto loginCatalogue = rdbms::Login::parseFile(cmdLineArgs.catalogueDbConfigPath);
  rdbms::ConnPool catalogueConnPool(loginCatalogue, maxNbConns);
  auto catalogueConn = catalogueConnPool.getConn();

  checkCatalogueSchema(&catalogueConn, loginCatalogue.dbType);

  // Compute the statistics
  auto catalogueStatisticsService = StatisticsServiceFactory::create(&catalogueConn, loginCatalogue.dbType);
  std::unique_ptr<Statistics> computedStatistics = catalogueStatisticsService->getStatistics();
  try {
    // output the statistics in json format
    statisticsStdoutJsonService->saveStatistics(*computedStatistics);
  }
  catch (cta::exception::Exception& ex) {
    std::cerr << ex.getMessageValue() << std::endl;
    if (computedStatistics != nullptr) {
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
void StatisticsSaveCmd::printUsage(std::ostream& os) {
  StatisticsSaveCmdLineArgs::printUsage(os);
}

void StatisticsSaveCmd::verifyCmdLineArgs(const StatisticsSaveCmdLineArgs& cmdLineArgs) const {
  if (cmdLineArgs.catalogueDbConfigPath.empty()) {
    throw cta::exception::Exception(
      "You should provide the catalogue database and the statistics database connection files.");
  }
}

void StatisticsSaveCmd::checkCatalogueSchema(cta::rdbms::Conn* catalogueConn, cta::rdbms::Login::DbType dbType) {
  using cta::catalogue::SchemaCheckerResult;
  // Check that the catalogue contains the table TAPE with the columns we need
  cta::catalogue::SchemaChecker::Builder catalogueCheckerBuilder("catalogue", dbType, *catalogueConn);
  std::unique_ptr<cta::catalogue::SchemaChecker> catalogueChecker = catalogueCheckerBuilder.build();

  SchemaCheckerResult result = catalogueChecker->checkTableContainsColumns(
    "TAPE", {"NB_MASTER_FILES", "MASTER_DATA_IN_BYTES", "NB_COPY_NB_1", "COPY_NB_1_IN_BYTES", "NB_COPY_NB_GT_1",
             "COPY_NB_GT_1_IN_BYTES", "DIRTY"});
  result += catalogueChecker->checkTableContainsColumns("TAPE_POOL", {"VIRTUAL_ORGANIZATION_ID"});

  if (result.getStatus() == SchemaCheckerResult::FAILED) {
    result.displayErrors(std::cerr);
    throw cta::exception::Exception("Catalogue schema checking failed.");
  }
}

}  // namespace statistics
}  // namespace cta
