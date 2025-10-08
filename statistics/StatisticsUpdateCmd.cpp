/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <cstdlib>
#include <memory>

#include "catalogue/SchemaChecker.hpp"
#include "common/Timer.hpp"
#include "rdbms/ConnPool.hpp"
#include "statistics/StatisticsUpdateCmd.hpp"
#include "statistics/StatisticsUpdateCmdLineArgs.hpp"
#include "StatisticsService.hpp"
#include "StatisticsServiceFactory.hpp"

namespace cta::statistics {

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int StatisticsUpdateCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  // using namespace cta::catalogue;
  const StatisticsUpdateCmdLineArgs cmdLineArgs(argc, argv);

  if (cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const uint64_t maxNbConns = 1;

  // Connect to the database
  auto loginCatalogue = rdbms::Login::parseFile(cmdLineArgs.catalogueDbConfigPath);
  rdbms::ConnPool catalogueConnPool(loginCatalogue, maxNbConns);
  auto catalogueConn = catalogueConnPool.getConn();

  // hecks catalogue database content to perform the update of the TAPE statistics
  cta::catalogue::SchemaChecker::Builder catalogueCheckerBuilder("catalogue", loginCatalogue.dbType, catalogueConn);
  std::unique_ptr<cta::catalogue::SchemaChecker> catalogueChecker;
  catalogueChecker = catalogueCheckerBuilder.build();

  catalogue::SchemaCheckerResult result = catalogueChecker->checkTableContainsColumns("TAPE", {"VID",
                                                                                               "DIRTY",
                                                                                               "NB_MASTER_FILES",
                                                                                               "MASTER_DATA_IN_BYTES",
                                                                                               "NB_COPY_NB_1",
                                                                                               "COPY_NB_1_IN_BYTES",
                                                                                               "NB_COPY_NB_GT_1",
                                                                                               "COPY_NB_GT_1_IN_BYTES"
                                                                                              });
  result += catalogueChecker->checkTableContainsColumns("TAPE_FILE", {"VID", "ARCHIVE_FILE_ID", "FSEQ", "COPY_NB"});
  result += catalogueChecker->checkTableContainsColumns("ARCHIVE_FILE", {"ARCHIVE_FILE_ID", "SIZE_IN_BYTES"});

  if (result.getStatus() == catalogue::SchemaCheckerResult::Status::FAILED) {
    result.displayErrors(std::cerr);
    return EXIT_FAILURE;
  }

  // Create the Statistics service
  std::unique_ptr<StatisticsService> service = StatisticsServiceFactory::create(&catalogueConn, loginCatalogue.dbType);
  // Update TAPE statistics
  std::cout << "Updating tape statistics in the catalogue..." << std::endl;
  cta::utils::Timer t;
  service->updateStatisticsPerTape();
  std::cout << "Updated catalogue tape statistics in " << t.secs() << " seconds, "
            << service->getNbUpdatedTapes() << " tape(s) have been updated" << std::endl;

  return EXIT_SUCCESS;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void StatisticsUpdateCmd::printUsage(std::ostream &os) {
  StatisticsUpdateCmdLineArgs::printUsage(os);
}

} // namespace cta::statistics
