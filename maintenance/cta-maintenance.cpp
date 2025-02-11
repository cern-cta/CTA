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

#include "common/exception/Errnum.hpp"
#include "common/config/Config.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/utils/utils.hpp"


#include "DiskReporting/DiskReportRunner.hpp"
#include "RepackRequestManager/RepackRequestManager.hpp"
#include "QueueCleanup/QueueCleanupRunner.hpp"

#include "objectstore/GarbageCollector.hpp"

#include "scheduler/Scheduler.hpp"
#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "rdbms/Login.hpp"

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

#include <string>


//------------------------------------------------------------------------------
// The help string
//------------------------------------------------------------------------------
const std::string gHelpString =
    "Usage: cta-maintenance [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t--foreground             or -f         \tRemain in the Foreground\n"
    "\t--stdout                 or -s         \tPrint logs to standard output. Required --foreground\n"
    "\t--log-to-file <log-file> or -l         \tLogs to a given file (instead of default syslog)\n"
    "\t--log-format <format>    or -o         \tOutput format for log messages (default or json)\n"
    "\t--config <config-file>   or -c         \tConfiguration file\n"
    "\t--help                   or -h         \tPrint this help and exit\n";

static struct option longopts[] = {
  // { .name, .has_args, .flag, .val } (see getopt.h))
  { "foreground", no_argument, nullptr, 'f' },
  { "config", required_argument, nullptr, 'c' },
  { "help", no_argument, nullptr, 'h' },
  { "stdout", no_argument, nullptr, 's' },
  { "log-to-file", required_argument, nullptr, 'l' },
  { "log-format", required_argument, nullptr, 'o' },
  { nullptr, 0, nullptr, '\0' }
};



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
static int exceptionThrowingMain(const common::Config config, cta::log::Logger &log);
void maintenanceLoop(DiskReportRunner& drr, RepackRequestManager& rrm, QueueCleanupRunner& qcr, objectstore::GarbageCollector& gc, log::LogContext& lc);


void maintenanceLoop(DiskReportRunner& drr, RepackRequestManager& rrm, QueueCleanupRunner& qcr, objectstore::GarbageCollector& gc, log::LogContext& lc){
  // Run the maintenance in a loop: queue cleanup, garbage collector and disk reporter
  try {
    do {
      utils::Timer t;
      qcr.runOnePass(lc);
      gc.runOnePass(lc);
      drr.runOnePass(lc);
      rrm.runOnePass(lc, 2);
      lc.log(log::INFO, "Did one round of cleaning. Sleeping for X seconds.");
      sleep(1);
    } while (true);
    lc.log(log::INFO, "In Maintenance::maintenanceLoop(): Received shutdown message. Exiting.");
  } catch(cta::exception::Exception & ex) {
    log::ScopedParamContainer exParams(lc);
    exParams.add("exceptionMessage", ex.getMessageValue());
    lc.log(log::ERR,
        "In Maintenance::maintenanceLoop(): received an exception. Backtrace follows.");

    lc.logBacktrace(log::INFO, ex.backtrace());
    throw ex;
  } catch(std::exception &ex) {
    log::ScopedParamContainer exParams(lc);
    exParams.add("exceptionMessage", ex.what());
    lc.log(log::ERR, "In Maintenance::maintenanceLoop(): received a std::exception.");
    throw ex;
  } catch(...) {
    lc.log(log::ERR, "In Maintenance::maintenanceLoop(): received an unknown exception.");
    throw;
  }
}


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
    // TODO: we have hardcoded the mount policy parameters here temporarily we will remove them once we know where to put them
    scheduler = std::make_unique<cta::Scheduler>(*catalogue, *sched_db, 5, 2*1000*1000);

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

  // Check daemon is being launched with cta:tape user:group IDs.

  // Options
  bool foreground = false;
  bool logToStdout = false;
  bool logToFile = false;
  std::string logFilePath;
  std::string logFormat;
  std::string configFileLocation;

  char c;
  // Reset getopt's global variables to make sure we start fresh
  optind=0;
  // Prevent getopt from printing out errors on stdout
  opterr=0;
  // We ask getopt to not reshuffle argv ('+')
  while ((c = getopt_long(argc, argv, "+fsc:l:h", longopts, nullptr)) != -1) {
    switch (c) {
    case 'f':
      foreground = true;
      break;
    case 's':
      logToStdout = true;
      break;
    case 'c':
      configFileLocation = optarg;
      break;
    case 'h':
      std::cout << gHelpString << std:: endl;
      return EXIT_SUCCESS;
    case 'l':
      logFilePath = optarg;
      logToFile = true;
      break;
    case 'o':
      logFormat = optarg;
      break;
    default:
      break;
    }
  }

  if(foreground)
	  return EXIT_FAILURE;

  std::string shortHostName;
  try {
    shortHostName = utils::getShortHostname();
  } catch (const exception::Errnum &ex) {
    std::cerr << "Failed to get short host name." << ex.getMessage().str();
    return EXIT_FAILURE;
  }

  std::unique_ptr<log::Logger> logPtr;
  logPtr.reset(new log::StdoutLogger(shortHostName, "cta-maintenance"));

  try {
    if(logToFile) {
      logPtr.reset(new log::FileLogger(shortHostName, "cta-maintenance", logFilePath, log::DEBUG));
    } else if(logToStdout) {
      logPtr.reset(new log::StdoutLogger(shortHostName, "cta-maintenance"));
    }
    if (! logFormat.empty()) {
      logPtr->setLogFormat(logFormat);
    }
  } catch (exception::Exception& ex) {
    std::cerr << "Failes to instantiate object representing CTA logging system: " << ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  }

  log::Logger& log = *logPtr;

  const common::Config config(configFileLocation);

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

