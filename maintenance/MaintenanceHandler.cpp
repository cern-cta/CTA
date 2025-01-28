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

#include <signal.h>
#include <sys/wait.h>
#include <sys/prctl.h>

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/exception/Errnum.hpp"
#include "rdbms/Login.hpp"
#include "scheduler/DiskReportRunner.hpp"
#include "scheduler/RepackRequestManager.hpp"
#include "scheduler/Scheduler.hpp"
#include "maintenance/MaintenanceHandler.hpp"

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

namespace cta::maintenance {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MaintenanceHandler::MaintenanceHandler(const configuration::MaintenanceConfiguration& maintenanceConfig):
SubprocessHandler("maintenanceHandler"), m_maintenanceConfig(maintenanceConfig) {
}

void MaintenanceHandler::exceptionThrowingMain(){
  // Open connections to the catalogue and object store, and run the garbage collector.

  // Set the thread name for process ID:
  prctl(PR_SET_NAME, "cta-maintenance");

  // Before anything, we will check for access to the scheduler's central storage.
  SchedulerDBInit_t sched_db_init("Maintenance", m_tapedConfig.backendPath.value(), m_processManager.logContext().logger());

  std::unique_ptr<cta::SchedulerDB_t> sched_db;
  std::unique_ptr<cta::catalogue::Catalogue> catalogue;
  std::unique_ptr<cta::Scheduler> scheduler;
  try {
    const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile(m_tapedConfig.fileCatalogConfigFile.value());
    const uint64_t nbConns = 1;
    const uint64_t nbArchiveFileListingConns = 1;
    auto catalogueFactory = cta::catalogue::CatalogueFactoryFactory::create(
      m_processManager.logContext().logger(),
      catalogueLogin,
      nbConns,
      nbArchiveFileListingConns);

    catalogue = catalogueFactory->create();
    sched_db = sched_db_init.getSchedDB(
      *catalogue,
      m_processManager.logContext().logger());

    // Set Scheduler DB cache timeouts
    SchedulerDatabase::StatisticsCacheConfig statisticsCacheConfig;
    statisticsCacheConfig.tapeCacheMaxAgeSecs = m_tapedConfig.tapeCacheMaxAgeSecs.value();
    statisticsCacheConfig.retrieveQueueCacheMaxAgeSecs = m_tapedConfig.retrieveQueueCacheMaxAgeSecs.value();
    sched_db->setStatisticsCacheConfig(statisticsCacheConfig);
    // TODO: we have hardcoded the mount policy parameters here temporarily we will remove them once we know where to put them
    scheduler = std::make_unique<cta::Scheduler>(*catalogue, *sched_db, 5, 2*1000*1000);

    // Before launching the maintenance loop, we validate that the scheduler is reachable.
    scheduler->ping(m_processManager.logContext());
  } catch(cta::exception::Exception &ex) {
    log::ScopedParamContainer exParams(m_processManager.logContext());
    exParams.add("errorMessage", ex.getMessageValue());
    m_processManager.logContext().log(log::CRIT,
          "In MaintenanceHandler::exceptionThrowingMain(): failed to contact central storage. Exiting.");
    throw ex;
  }

  // Create the garbage collector and the disk reporter
  auto gc = sched_db_init.getGarbageCollector(*catalogue);
  auto cleanupRunner = sched_db_init.getQueueCleanupRunner(*catalogue, *sched_db);
  DiskReportRunner diskReportRunner(*scheduler);
  RepackRequestManager repackRequestManager(*scheduler);

  if(!runRepackRequestManager()){
    m_processManager.logContext().log(log::INFO,
    "In MaintenanceHandler::exceptionThrowingMain(): Repack management is disabled. No repack-related operations will run on this maintenance process.");
  }

  // Run the maintenance in a loop: queue cleanup, garbage collector and disk reporter
  try {
    do {
      utils::Timer t;
      m_processManager.logContext().log(log::DEBUG,
          "In MaintenanceHandler::exceptionThrowingMain(): About to do a maintenance pass.");
      cleanupRunner.runOnePass(m_processManager.logContext());
      gc.runOnePass(m_processManager.logContext());
      diskReportRunner.runOnePass(m_processManager.logContext());
      m_processManager.logContext().log(log::DEBUG,
                                        "In MaintenanceHandler::exceptionThrowingMain(): After diskReportRunner.runOnePass().");
      if(runRepackRequestManager()){
        repackRequestManager.runOnePass(m_processManager.logContext(), m_tapedConfig.repackMaxRequestsToExpand.value());
      }
      m_processManager.logContext().log(log::DEBUG,
                                        "In MaintenanceHandler::exceptionThrowingMain(): After runRepackRequestManager().");
    } while (!receivedShutdownMessage);
    m_processManager.logContext().log(log::INFO,
        "In MaintenanceHandler::exceptionThrowingMain(): Received shutdown message. Exiting.");
  } catch(cta::exception::Exception & ex) {
    log::ScopedParamContainer exParams(m_processManager.logContext());
    exParams.add("exceptionMessage", ex.getMessageValue());
    m_processManager.logContext().log(log::ERR,
        "In MaintenanceHandler::exceptionThrowingMain(): received an exception. Backtrace follows.");

    m_processManager.logContext().logBacktrace(log::INFO, ex.backtrace());
    throw ex;
  } catch(std::exception &ex) {
    log::ScopedParamContainer exParams(m_processManager.logContext());
    exParams.add("exceptionMessage", ex.what());
    m_processManager.logContext().log(log::ERR,
        "In MaintenanceHandler::exceptionThrowingMain(): received a std::exception.");
    throw ex;
  } catch(...) {
    m_processManager.logContext().log(log::ERR,
        "In MaintenanceHandler::exceptionThrowingMain(): received an unknown exception.");
    throw;
  }
}
} // namespace cta::maintenance
