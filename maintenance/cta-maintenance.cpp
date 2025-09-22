/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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


#include <getopt.h>
#include <string>

#include "diskbufferrunners/DiskReportRunner.hpp"
#include "repackrequestrunner/RepackRequestManager.hpp"
#include "osrunners/QueueCleanupRunner.hpp"
#include "osrunners/GarbageCollector.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/exception/Errnum.hpp"
#include "common/CmdLineParams.hpp"
#include "common/config/Config.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/Login.hpp"
#include "scheduler/Scheduler.hpp"
#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif


namespace cta::maintenance {

//------------------------------------------------------------------------------
// exceptionThrowingMain
//
// The main() function delegates the bulk of its implementation to this
// exception throwing version.
//
// @param argc The number of command-line arguments.
// @param argv The command-line arguments.
// @param log The logging system.
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const common::Config config, cta::log::Logger& log) {
  log::LogContext lc(log);

  // Before anything, we will check for access to the scheduler's central storage.
  SchedulerDBInit_t sched_db_init("Maintenance", config.getOptionValueStr("BackendPath").value(), lc.logger());

  std::unique_ptr<cta::SchedulerDB_t> sched_db;
  std::unique_ptr<cta::catalogue::Catalogue> catalogue;
  std::unique_ptr<cta::Scheduler> scheduler;
  try {
    const rdbms::Login catalogueLogin = rdbms::Login::parseFile(config.getOptionValueStr("CatalogueConfigFile").value());
    const uint64_t nbConns = 1;
    const uint64_t nbArchiveFileListingConns = 1;
    auto catalogueFactory = cta::catalogue::CatalogueFactoryFactory::create(
      lc.logger(),
      catalogueLogin,
      nbConns,
      nbArchiveFileListingConns);

    catalogue = catalogueFactory->create();
    sched_db = sched_db_init.getSchedDB(
      *catalogue,
      lc.logger());

    // Set Scheduler DB cache timeouts
    SchedulerDatabase::StatisticsCacheConfig statisticsCacheConfig;
    statisticsCacheConfig.tapeCacheMaxAgeSecs = config.getOptionValueInt("TapeCacheMaxAgeSecs").value();
    statisticsCacheConfig.retrieveQueueCacheMaxAgeSecs = config.getOptionValueInt("RetrieveQueueCacheMaxAgeSecs").value();
    sched_db->setStatisticsCacheConfig(statisticsCacheConfig);
    scheduler = std::make_unique<cta::Scheduler>(*catalogue, *sched_db, config.getOptionValueStr("SchedulerBackendName").value());

    // Before launching the maintenance loop, we validate that the scheduler is reachable.
    scheduler->ping(lc);
  } catch(cta::exception::Exception &ex) {
    log::ScopedParamContainer exParams(lc);
    exParams.add("errorMessage", ex.getMessageValue());
    lc.log(log::CRIT,
          "In MaintenanceServer::exceptionThrowingMain(): failed to contact central storage. Exiting.");
    throw ex;
  }

  auto gc = sched_db_init.getGarbageCollector(*catalogue);
  auto cleanupRunner = sched_db_init.getQueueCleanupRunner(*catalogue, *sched_db);
  DiskReportRunner diskReportRunner(*scheduler);
  RepackRequestManager repackRequestManager(*scheduler);
  
  maintenanceLoop(diskReportRunner, repackRequestManager, cleanupRunner, gc ,lc);
  return 0;
}

} // namespace cta::maintenance

int main(const int argc, char **const argv) {
  using namespace cta;

  // Interpret the command line
  std::unique_ptr<common::CmdLineParams> cmdLineParams;
  commandLine.reset(new common::CmdLineParams(argc, argv, "cta-maintenance"));

  std::string shortHostName;
  try {
    shortHostName = utils::getShortHostname();
  } catch (const exception::Errnum &ex) {
    std::cerr << "Failed to get short host name." << ex.getMessage().str();
    return EXIT_FAILURE;
  }

  std::unique_ptr<log::Logger> logPtr;
  logPtr.reset(new log::StdoutLogger(shortHostName, "cta-maintenance"));

  const common::Config config(cmdLineParams->configFileLocation);

  // Change user and group
  const std::string userName = config.daemonUserName.value();
  const std::string groupName = config.daemonGroupName.value();

  try {
    (*logPtr)(log::INFO, "Setting user name and group name of current process",
                  {{"userName", userName}, {"groupName", groupName}});
    cta::System::setUserAndGroup(userName, groupName);
    // There is no longer any need for the process to be able to change user,
    // however the process should still be permitted to make the raw IO
    // capability effective in the future when needed.
    cta::server::ProcessCap::setProcText("cap_sys_rawio+p");

  } catch (exception::Exception& ex) {
    std::list<log::Param> params = {
      log::Param("exceptionMessage", ex.getMessage().str())};
    (*logPtr)(log::ERR, "Caught an unexpected CTA, cta-taped cannot start", params);
    return EXIT_FAILURE;
  }

  // Try to instantiate the logging system API
  try {
    if(cmdLineParams->logToFile) {
      logPtr.reset(new log::FileLogger(shortHostName, "cta-maintenance", cmdLineParams->logFilePath, log::DEBUG));
    } else if(cmdLineParams->logToStdout) {
      logPtr.reset(new log::StdoutLogger(shortHostName, "cta-maintenance"));
    }
    if (!cmdLineParams->logFormat.empty()) {
      logPtr->setLogFormat(cmdLineParams->logFormat);
    }
  } catch (exception::Exception& ex) {
    std::cerr << "Failed to instantiate object representing CTA logging system: " << ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  }

  log::Logger& log = *logPtr;

  int programRc = EXIT_FAILURE;
  try {
    programRc = maintenance::exceptionThrowingMain(config, log);
  }  catch(exception::Exception &ex) {
    std::list<cta::log::Param> params = {
      cta::log::Param("exceptionMessage", ex.getMessage().str())};
    log(log::ERR, "Caught an unexpected CTA exception, cta-maintenance cannot start", params);
    sleep(1);
  } catch(std::exception &se) {
    std::list<cta::log::Param> params = {cta::log::Param("what", se.what())};
    log(log::ERR, "Caught an unexpected standard exception, cta-maintenance cannot start", params);
    sleep(1);
  } catch(...) {
    log(log::ERR, "Caught an unexpected and unknown exception, cta-maintenance cannot start");
    sleep(1);
  }

  return programRc;
}

