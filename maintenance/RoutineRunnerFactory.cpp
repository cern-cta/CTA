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
#include <sys/prctl.h>
#include <chrono>
#include <thread>

#include "RoutineRunner.hpp"
#include "RoutineRunnerFactory.hpp"
#include "routines/disk/DiskReportRoutine.hpp"
#include "routines/repack/RepackRequestRoutine.hpp"
#include "routines/objectstore/QueueCleanupRoutine.hpp"
#include "routines/objectstore/GarbageCollectRoutine.hpp"

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/exception/Errnum.hpp"
#include "common/semconv/Attributes.hpp"
#include "rdbms/Login.hpp"

#include "common/exception/Exception.hpp"
#include "scheduler/Scheduler.hpp"
#include "catalogue/Catalogue.hpp"

#include "IRoutine.hpp"

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

namespace cta::maintenance {

std::unique_ptr<RoutineRunner> RoutineRunnerFactory::create(cta::log::LogContext& lc,
                                                            const cta::common::Config& config) {
  if (!config.getOptionValueStr("cta.objectstore.backendpath").has_value()) {
    throw InvalidConfiguration("Could not find config entry 'cta.objectstore.backendpath' in");
  }
  auto schedDbInit =
    std::make_unique<SchedulerDBInit_t>("Maintenance",
                                        config.getOptionValueStr("cta.objectstore.backendpath").value(),
                                        lc.logger());

  if (!config.getOptionValueStr("cta.catalogue.config_file").has_value()) {
    throw InvalidConfiguration("Could not find config entry 'cta.catalogue.config_file'");
  }
  const rdbms::Login catalogueLogin =
    rdbms::Login::parseFile(config.getOptionValueStr("cta.catalogue.config_file").value());
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 1;
  auto catalogueFactory =
    cta::catalogue::CatalogueFactoryFactory::create(lc.logger(), catalogueLogin, nbConns, nbArchiveFileListingConns);

  auto catalogue = catalogueFactory->create();
  auto scheddb = schedDbInit->getSchedDB(*catalogue, lc.logger());

  // Set Scheduler DB cache timeouts
  SchedulerDatabase::StatisticsCacheConfig statisticsCacheConfig;
  statisticsCacheConfig.tapeCacheMaxAgeSecs =
    config.getOptionValueInt("cta.schedulerdb.tape_cache_max_age_secs").value_or(600);
  statisticsCacheConfig.retrieveQueueCacheMaxAgeSecs =
    config.getOptionValueInt("cta.schedulerdb.retrieve_queue_cache_max_age_secs").value_or(10);
  scheddb->setStatisticsCacheConfig(statisticsCacheConfig);

  if (!config.getOptionValueStr("cta.scheduler_backend_name").has_value()) {
    throw InvalidConfiguration("Could not find config entry 'cta.scheduler_backend_name'");
  }
  auto scheduler = std::make_unique<cta::Scheduler>(*catalogue,
                                                    *scheddb,
                                                    config.getOptionValueStr("cta.scheduler_backend_name").value());

  uint32_t sleepInterval = config.getOptionValueUInt("cta.maintenance.sleep_interval").value_or(1000);
  std::unique_ptr<RoutineRunner> routineRunner = std::make_unique<RoutineRunner>(sleepInterval);

  // Register all of the different routines

  // Add Disk Reporter
  if (config.getOptionValueBool("cta.disk_reporter.enabled").value_or(true)) {
    routineRunner->registerRoutine(
      std::make_unique<DiskReportRoutine>(lc,
                                          *scheduler,
                                          config.getOptionValueInt("cta.disk_reporter.batch_size").value_or(500),
                                          config.getOptionValueInt("cta.disk_reporter.soft_timeout").value_or(30)));
  }

  // Add Garbage Collector
  if (config.getOptionValueBool("cta.garbage_collector.enabled").value_or(true)) {
#ifndef CTA_PGSCHED
    routineRunner->registerRoutine(
      std::make_unique<maintenance::GarbageCollectRoutine>(lc,
                                                           schedDbInit->getBackend(),
                                                           schedDbInit->getAgentReference(),
                                                           *catalogue));
#endif
  }

  // Add Queue Cleanup
  if (config.getOptionValueBool("cta.queue_cleanup.enabled").value_or(true)) {
#ifndef CTA_PGSCHED
    routineRunner->registerRoutine(std::make_unique<maintenance::QueueCleanupRoutine>(
      lc,
      schedDbInit->getAgentReference(),
      *scheddb,
      *catalogue,
      config.getOptionValueInt("cta.queue_cleanup.batch_size").value_or(500)));
#else
    routineRunner->registerRoutine(std::make_unique<maintenance::RelationalDBQCR>(*catalogue, *scheddb));
#endif
  }

  // Add Repack request manager
  if (config.getOptionValueBool("cta.repack.enabled").value_or(true)) {
    routineRunner->registerRoutine(std::make_unique<RepackRequestRoutine>(
      lc,
      *scheduler,
      config.getOptionValueInt("cta.repack.request_manager.max_to_toexpand").value_or(2),
      config.getOptionValueInt("cta.repack.reporter.soft_timeout").value_or(30)));
  }

  return routineRunner;
}

}  // namespace cta::maintenance
