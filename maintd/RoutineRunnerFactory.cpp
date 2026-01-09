/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RoutineRunnerFactory.hpp"

#include "IRoutine.hpp"
#include "RoutineRunner.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include "common/semconv/Attributes.hpp"
#include "rdbms/Login.hpp"
#include "routines/disk/DiskReportArchiveRoutine.hpp"
#include "routines/disk/DiskReportRetrieveRoutine.hpp"
#include "routines/repack/RepackExpandRoutine.hpp"
#include "routines/repack/RepackReportRoutine.hpp"
#ifndef CTA_PGSCHED
#include "routines/scheduler/objectstore/GarbageCollectRoutine.hpp"
#include "routines/scheduler/objectstore/QueueCleanupRoutine.hpp"
#endif

#include <chrono>
#include <signal.h>
#include <sys/prctl.h>
#include <thread>

namespace cta::maintd {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RoutineRunnerFactory::RoutineRunnerFactory(const cta::common::Config& config, cta::log::LogContext& lc)
    : m_config(config),
      m_lc(lc) {
  if (!m_config.getOptionValueStr("cta.catalogue.config_file").has_value()) {
    throw exception::UserError("Could not find config entry 'cta.catalogue.config_file'");
  }

  m_lc.log(log::INFO, "In RoutineRunnerFactory::RoutineRunnerFactory(): Initialising Catalogue");
  const rdbms::Login catalogueLogin =
    rdbms::Login::parseFile(m_config.getOptionValueStr("cta.catalogue.config_file").value());
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 1;
  auto catalogueFactory =
    cta::catalogue::CatalogueFactoryFactory::create(m_lc.logger(), catalogueLogin, nbConns, nbArchiveFileListingConns);

  m_catalogue = catalogueFactory->create();

  m_lc.log(log::INFO, "In RoutineRunnerFactory::RoutineRunnerFactory(): Initialising Scheduler");
  if (!m_config.getOptionValueStr("cta.objectstore.backendpath").has_value()) {
    throw exception::UserError("Could not find config entry 'cta.objectstore.backendpath' in");
  }
  m_schedDbInit = std::make_unique<SchedulerDBInit_t>("Maintd",
                                                      m_config.getOptionValueStr("cta.objectstore.backendpath").value(),
                                                      m_lc.logger());
  m_schedDb = m_schedDbInit->getSchedDB(*m_catalogue, m_lc.logger());
  // Set Scheduler DB cache timeouts
  SchedulerDatabase::StatisticsCacheConfig statisticsCacheConfig;
  statisticsCacheConfig.tapeCacheMaxAgeSecs =
    m_config.getOptionValueInt("cta.schedulerdb.tape_cache_max_age_secs").value_or(600);
  statisticsCacheConfig.retrieveQueueCacheMaxAgeSecs =
    m_config.getOptionValueInt("cta.schedulerdb.retrieve_queue_cache_max_age_secs").value_or(10);
  m_schedDb->setStatisticsCacheConfig(statisticsCacheConfig);

  if (!m_config.getOptionValueStr("cta.scheduler_backend_name").has_value()) {
    throw exception::UserError("Could not find config entry 'cta.scheduler_backend_name'");
  }
  m_scheduler = std::make_unique<cta::Scheduler>(*m_catalogue,
                                                 *m_schedDb,
                                                 m_config.getOptionValueStr("cta.scheduler_backend_name").value());
  m_lc.log(log::INFO, "In RoutineRunnerFactory::RoutineRunnerFactory(): Scheduler and Catalogue initialised");
}

//------------------------------------------------------------------------------
// RoutineRunnerFactory::create
//------------------------------------------------------------------------------
std::unique_ptr<RoutineRunner> RoutineRunnerFactory::create() {
  m_lc.log(log::INFO, "In RoutineRunnerFactory::create(): Creating RoutineRunner");

  uint32_t sleepInterval = m_config.getOptionValueUInt("cta.routines.sleep_interval").value_or(1000);
  auto routineRunner = std::make_unique<RoutineRunner>(sleepInterval);

  // Register all of the different routines

  // Add Disk Reporter for Archive
  if (m_config.getOptionValueBool("cta.routines.disk_report_archive.enabled").value_or(true)) {
    routineRunner->registerRoutine(std::make_unique<DiskReportArchiveRoutine>(
      m_lc,
      *m_scheduler,
      m_config.getOptionValueInt("cta.routines.disk_report_archive.batch_size").value_or(500),
      m_config.getOptionValueInt("cta.routines.disk_report_archive.soft_timeout").value_or(30)));
  }

  // Add Disk Reporter for Retrieve
  if (m_config.getOptionValueBool("cta.routines.disk_report_retrieve.enabled").value_or(true)) {
    routineRunner->registerRoutine(std::make_unique<DiskReportRetrieveRoutine>(
      m_lc,
      *m_scheduler,
      m_config.getOptionValueInt("cta.routines.disk_report_retrieve.batch_size").value_or(500),
      m_config.getOptionValueInt("cta.routines.disk_report_retrieve.soft_timeout").value_or(30)));
  }

  // Add Garbage Collector
  if (m_config.getOptionValueBool("cta.routines.garbage_collect.enabled").value_or(true)) {
#ifndef CTA_PGSCHED
    routineRunner->registerRoutine(std::make_unique<maintd::GarbageCollectRoutine>(m_lc,
                                                                                   m_schedDbInit->getBackend(),
                                                                                   m_schedDbInit->getAgentReference(),
                                                                                   *m_catalogue));
#endif
  }

  // Add Queue Cleanup
  if (m_config.getOptionValueBool("cta.routines.queue_cleanup.enabled").value_or(true)) {
#ifndef CTA_PGSCHED
    routineRunner->registerRoutine(std::make_unique<maintd::QueueCleanupRoutine>(
      m_lc,
      *m_schedDb,
      *m_catalogue,
      m_config.getOptionValueInt("cta.routines.queue_cleanup.batch_size").value_or(500)));
#endif
  }

  // Add Repack Expansion
  if (m_config.getOptionValueBool("cta.routines.repack_expand.enabled").value_or(true)) {
    routineRunner->registerRoutine(std::make_unique<RepackExpandRoutine>(
      m_lc,
      *m_scheduler,
      m_config.getOptionValueInt("cta.routines.repack_expand.max_to_toexpand").value_or(2)));
  }

  // Add Repack Reporting
  if (m_config.getOptionValueBool("cta.routines.repack_report.enabled").value_or(true)) {
    routineRunner->registerRoutine(std::make_unique<RepackReportRoutine>(
      m_lc,
      *m_scheduler,
      m_config.getOptionValueInt("cta.routines.repack_report.soft_timeout").value_or(30)));
  }

  m_lc.log(log::INFO, "In RoutineRunnerFactory::create(): RoutineRunner created");
  return routineRunner;
}

}  // namespace cta::maintd
