/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RoutineRegistrar.hpp"

#include "IRoutine.hpp"
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
#else
#include "routines/scheduler/rdbms/AncientRowRoutines.hpp"
#include "routines/scheduler/rdbms/InactiveMountQueueRoutines.hpp"
#endif

#include <chrono>
#include <signal.h>
#include <sys/prctl.h>
#include <thread>

namespace cta::maintd {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RoutineRegistrar::RoutineRegistrar(const MaintdConfig& config, cta::log::LogContext& lc) : m_config(config), m_lc(lc) {
  m_lc.log(log::INFO, "In RoutineRegistrar::RoutineRegistrar(): Initialising Catalogue");
  const rdbms::Login catalogueLogin = rdbms::Login::parseFile(m_config.catalogue.config_file);
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 1;
  auto catalogueFactory =
    cta::catalogue::CatalogueFactoryFactory::create(m_lc.logger(), catalogueLogin, nbConns, nbArchiveFileListingConns);

  m_catalogue = catalogueFactory->create();

  m_lc.log(log::INFO, "In RoutineRegistrar::RoutineRegistrar(): Initialising Scheduler");

  // TODO: why are we manually putting "maintd" here?
  m_schedDbInit =
    std::make_unique<SchedulerDBInit_t>("Maintd", m_config.scheduler.objectstore_backend_path, m_lc.logger());
  m_schedDb = m_schedDbInit->getSchedDB(*m_catalogue, m_lc.logger());
  // Set Scheduler DB cache timeouts
  SchedulerDatabase::StatisticsCacheConfig statisticsCacheConfig;
  statisticsCacheConfig.tapeCacheMaxAgeSecs = m_config.scheduler.tape_cache_max_age_secs;
  statisticsCacheConfig.retrieveQueueCacheMaxAgeSecs = m_config.scheduler.retrieve_queue_cache_max_age_secs;
  m_schedDb->setStatisticsCacheConfig(statisticsCacheConfig);

  m_scheduler = std::make_unique<cta::Scheduler>(*m_catalogue, *m_schedDb, config.scheduler.backend_name);
  m_lc.log(log::INFO, "In RoutineRegistrar::RoutineRegistrar(): Scheduler and Catalogue initialised");
}

//------------------------------------------------------------------------------
// RoutineRegistrar::registerRoutines
//------------------------------------------------------------------------------
void RoutineRegistrar::registerRoutines(RoutineRunner& routineRunner) {
  m_lc.log(log::INFO, "In RoutineRegistrar::registerRoutines(): Registring routines");

  // Register all of the different routines

  // Add Disk Reporter for Archive
  if (m_config.routines.disk_report_archive.enabled) {
    routineRunner.registerRoutine(
      std::make_unique<DiskReportArchiveRoutine>(m_lc,
                                                 *m_scheduler,
                                                 m_config.routines.disk_report_archive.batch_size,
                                                 m_config.routines.disk_report_archive.soft_timeout_secs));
  }

  // Add Disk Reporter for Retrieve
  if (m_config.routines.disk_report_retrieve.enabled) {
    routineRunner.registerRoutine(
      std::make_unique<DiskReportRetrieveRoutine>(m_lc,
                                                  *m_scheduler,
                                                  m_config.routines.disk_report_retrieve.batch_size,
                                                  m_config.routines.disk_report_retrieve.soft_timeout_secs));
  }

/*
 * If we enable all routines in 1 process they will all be running sequentially
 * and with the same sleep_interval configured for the maintd process itself.
 * We can currently achieve separate sleep intervals per routine (/group of routines)
 * only if we deploy multiple processes with some routines disabled and different
 * sleep_interval in their configuration file. It might probably make sense to
 * update the internal model of maintd in the future as well
 * (or fully split it into separate services; 1 per routine).
 */
#ifndef CTA_PGSCHED
  // Add Garbage Collector
  if (m_config.routines.garbage_collect.enabled) {
    routineRunner.registerRoutine(std::make_unique<GarbageCollectRoutine>(m_lc,
                                                                          m_schedDbInit->getBackend(),
                                                                          m_schedDbInit->getAgentReference(),
                                                                          *m_catalogue));
  }
  // Add Queue Cleanup
  if (m_config.routines.queue_cleanup.enabled) {
    routineRunner.registerRoutine(std::make_unique<QueueCleanupRoutine>(m_lc,
                                                                        *m_schedDb,
                                                                        *m_catalogue,
                                                                        m_config.routines.queue_cleanup.batch_size));
  }

  // Add Repack Expansion
  if (m_config.routines.repack_expand.enabled) {
    routineRunner.registerRoutine(
      std::make_unique<RepackExpandRoutine>(m_lc, *m_scheduler, m_config.routines.repack_expand.max_to_toexpand));
  }

  // Add Repack Reporting
  if (m_config.routines.repack_report.enabled) {
    routineRunner.registerRoutine(
      std::make_unique<RepackReportRoutine>(m_lc, *m_scheduler, m_config.routines.repack_report.soft_timeout_secs));
  }
#else
  // Add User Archive and Retrieve Active Queue Cleanup
  if (m_config.routines.user_active_queue_cleanup.enabled) {
    routineRunner.registerRoutine(std::make_unique<ArchiveInactiveMountActiveQueueRoutine>(
      m_lc,
      *m_catalogue,
      *m_schedDb,
      m_config.routines.user_active_queue_cleanup.batch_size,
      m_config.routines.user_active_queue_cleanup.age_for_collection_secs));
    routineRunner.registerRoutine(std::make_unique<RetrieveInactiveMountActiveQueueRoutine>(
      m_lc,
      *m_catalogue,
      *m_schedDb,
      m_config.routines.user_active_queue_cleanup.batch_size,
      m_config.routines.user_active_queue_cleanup.age_for_collection_secs));
  }
  // Add Repack Archive and Repack Retrieve Active Queue Cleanup
  if (m_config.routines.repack_active_queue_cleanup.enabled) {
    routineRunner.registerRoutine(std::make_unique<RepackArchiveInactiveMountActiveQueueRoutine>(
      m_lc,
      *m_catalogue,
      *m_schedDb,
      m_config.routines.repack_active_queue_cleanup.batch_size,
      m_config.routines.repack_active_queue_cleanup.age_for_collection_secs));
    routineRunner.registerRoutine(std::make_unique<RepackRetrieveInactiveMountActiveQueueRoutine>(
      m_lc,
      *m_catalogue,
      *m_schedDb,
      m_config.routines.repack_active_queue_cleanup.batch_size,
      m_config.routines.repack_active_queue_cleanup.age_for_collection_secs));
  }
  // Add User Archive and Repack Retrieve Pending Queue Cleanup
  if (m_config.routines.user_pending_queue_cleanup.enabled) {
    routineRunner.registerRoutine(std::make_unique<ArchiveInactiveMountPendingQueueRoutine>(
      m_lc,
      *m_catalogue,
      *m_schedDb,
      m_config.routines.user_pending_queue_cleanup.batch_size,
      m_config.routines.user_pending_queue_cleanup.age_for_collection_secs));
    routineRunner.registerRoutine(std::make_unique<RetrieveInactiveMountPendingQueueRoutine>(
      m_lc,
      *m_catalogue,
      *m_schedDb,
      m_config.routines.user_pending_queue_cleanup.batch_size,
      m_config.routines.user_pending_queue_cleanup.age_for_collection_secs));
  }
  // Add Repack Archive and Repack Retrieve Pending Queue Cleanup
  if (m_config.routines.repack_pending_queue_cleanup.enabled) {
    routineRunner.registerRoutine(std::make_unique<RepackArchiveInactiveMountPendingQueueRoutine>(
      m_lc,
      *m_catalogue,
      *m_schedDb,
      m_config.routines.repack_pending_queue_cleanup.batch_size,
      m_config.routines.repack_pending_queue_cleanup.age_for_collection_secs));
    routineRunner.registerRoutine(std::make_unique<RepackRetrieveInactiveMountPendingQueueRoutine>(
      m_lc,
      *m_catalogue,
      *m_schedDb,
      m_config.routines.repack_pending_queue_cleanup.batch_size,
      m_config.routines.repack_pending_queue_cleanup.age_for_collection_secs));
  }
  // Add Scheduler Maintenance Cleanup
  if (m_config.routines.scheduler_maintenance_cleanup.enabled) {
    routineRunner.registerRoutine(std::make_unique<DeleteOldFailedQueuesRoutine>(
      m_lc,
      *m_schedDb,
      m_config.routines.scheduler_maintenance_cleanup.batch_size,
      m_config.routines.scheduler_maintenance_cleanup.age_for_deletion_secs));
    routineRunner.registerRoutine(std::make_unique<CleanMountLastFetchTimeRoutine>(
      m_lc,
      *m_schedDb,
      m_config.routines.scheduler_maintenance_cleanup.batch_size,
      m_config.routines.scheduler_maintenance_cleanup.age_for_deletion_secs));
  }
#endif

  m_lc.log(log::INFO, "In RoutineRegistrar::registerRoutines(): Routines registered");
}

}  // namespace cta::maintd
