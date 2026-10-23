/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RoutineRunnerFactory.hpp"

#include "IRoutine.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include "common/semconv/Attributes.hpp"
#include "routines/disk/DiskReportArchiveRoutine.hpp"
#include "routines/disk/DiskReportRetrieveRoutine.hpp"
#include "routines/repack/RepackExpandRoutine.hpp"
#include "routines/repack/RepackReportRoutine.hpp"
#ifndef CTA_PGSCHED
#include "routines/scheduler/objectstore/GarbageCollectRoutine.hpp"
#include "routines/scheduler/objectstore/QueueCleanupRoutine.hpp"
#else
#include "routines/scheduler/rdbms/AncientRowRoutines.hpp"
#include "routines/scheduler/rdbms/DiskSystemSleepRoutines.hpp"
#include "routines/scheduler/rdbms/InactiveMountQueueRoutines.hpp"
#include "routines/scheduler/rdbms/ReportingCleanupRoutines.hpp"
#endif

#include <chrono>
#include <signal.h>
#include <sys/prctl.h>
#include <thread>

namespace cta::maintd {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RoutineRunnerFactory::RoutineRunnerFactory(const MaintdConfig& config,
                                           service_resources::ServiceResources& resources,
                                           cta::log::LogContext& lc)
    : m_config(config),
      m_lc(lc),
      m_resources(resources) {}

//------------------------------------------------------------------------------
// RoutineRunnerFactory::registerRoutines
//------------------------------------------------------------------------------
std::unique_ptr<RoutineRunner> RoutineRunnerFactory::create() {
  m_lc.log(log::INFO, "In RoutineRunnerFactory::registerRoutines(): Creating routines");

  // Create all of the different routines

  std::vector<std::unique_ptr<IRoutine>> routines;

  // Add Disk Reporter for Archive
  if (m_config.routines.disk_report_archive.enabled) {
    routines.push_back(
      std::make_unique<DiskReportArchiveRoutine>(m_lc,
                                                 m_resources.getScheduler(),
                                                 m_config.routines.disk_report_archive.batch_size,
                                                 m_config.routines.disk_report_archive.soft_timeout_secs));
  }

  // Add Disk Reporter for Retrieve
  if (m_config.routines.disk_report_retrieve.enabled) {
    routines.push_back(
      std::make_unique<DiskReportRetrieveRoutine>(m_lc,
                                                  m_resources.getScheduler(),
                                                  m_config.routines.disk_report_retrieve.batch_size,
                                                  m_config.routines.disk_report_retrieve.soft_timeout_secs));
  }

  // Add Repack Expansion
  if (m_config.routines.repack_expand.enabled) {
    routines.push_back(std::make_unique<RepackExpandRoutine>(m_lc,
                                                             m_resources.getScheduler(),
                                                             m_config.routines.repack_expand.max_to_expand));
  }

  // Add Repack Reporting
  if (m_config.routines.repack_report.enabled) {
    routines.push_back(std::make_unique<RepackReportRoutine>(m_lc,
                                                             m_resources.getScheduler(),
                                                             m_config.routines.repack_report.soft_timeout_secs));
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
    routines.push_back(std::make_unique<GarbageCollectRoutine>(m_lc,
                                                               m_resources.getSchedDbInit().getBackend(),
                                                               m_resources.getSchedDbInit().getAgentReference(),
                                                               m_resources.getCatalogue()));
  }
  // Add Queue Cleanup
  if (m_config.routines.queue_cleanup.enabled) {
    routines.push_back(std::make_unique<QueueCleanupRoutine>(m_lc,
                                                             m_resources.getSchedDb(),
                                                             m_resources.getCatalogue(),
                                                             m_config.routines.queue_cleanup.batch_size));
  }
#else
  // Add User Archive and Retrieve Active Queue Cleanup
  if (m_config.routines.user_active_queue_cleanup.enabled) {
    routines.push_back(std::make_unique<ArchiveInactiveMountActiveQueueRoutine>(
      m_lc,
      m_resources.getCatalogue(),
      m_resources.getSchedDb(),
      m_config.routines.user_active_queue_cleanup.batch_size,
      m_config.routines.user_active_queue_cleanup.age_for_collection_secs));
    routines.push_back(std::make_unique<RetrieveInactiveMountActiveQueueRoutine>(
      m_lc,
      m_resources.getCatalogue(),
      m_resources.getSchedDb(),
      m_config.routines.user_active_queue_cleanup.batch_size,
      m_config.routines.user_active_queue_cleanup.age_for_collection_secs));
    routines.push_back(std::make_unique<ResubmitInactiveReportingRoutine>(
      m_lc,
      m_resources.getSchedDb(),
      m_config.routines.user_active_queue_cleanup.batch_size,
      m_config.routines.user_active_queue_cleanup.age_for_collection_secs));
  }
  // Add Repack Archive and Repack Retrieve Active Queue Cleanup
  if (m_config.routines.repack_active_queue_cleanup.enabled) {
    routines.push_back(std::make_unique<RepackArchiveInactiveMountActiveQueueRoutine>(
      m_lc,
      m_resources.getCatalogue(),
      m_resources.getSchedDb(),
      m_config.routines.repack_active_queue_cleanup.batch_size,
      m_config.routines.repack_active_queue_cleanup.age_for_collection_secs));
    routines.push_back(std::make_unique<RepackRetrieveInactiveMountActiveQueueRoutine>(
      m_lc,
      m_resources.getCatalogue(),
      m_resources.getSchedDb(),
      m_config.routines.repack_active_queue_cleanup.batch_size,
      m_config.routines.repack_active_queue_cleanup.age_for_collection_secs));
  }
  // Add User Archive and Repack Retrieve Pending Queue Cleanup
  if (m_config.routines.user_pending_queue_cleanup.enabled) {
    routines.push_back(std::make_unique<ArchiveInactiveMountPendingQueueRoutine>(
      m_lc,
      m_resources.getCatalogue(),
      m_resources.getSchedDb(),
      m_config.routines.user_pending_queue_cleanup.batch_size,
      m_config.routines.user_pending_queue_cleanup.age_for_collection_secs));
    routines.push_back(std::make_unique<RetrieveInactiveMountPendingQueueRoutine>(
      m_lc,
      m_resources.getCatalogue(),
      m_resources.getSchedDb(),
      m_config.routines.user_pending_queue_cleanup.batch_size,
      m_config.routines.user_pending_queue_cleanup.age_for_collection_secs));
  }
  // Add Repack Archive and Repack Retrieve Pending Queue Cleanup
  if (m_config.routines.repack_pending_queue_cleanup.enabled) {
    routines.push_back(std::make_unique<RepackArchiveInactiveMountPendingQueueRoutine>(
      m_lc,
      m_resources.getCatalogue(),
      m_resources.getSchedDb(),
      m_config.routines.repack_pending_queue_cleanup.batch_size,
      m_config.routines.repack_pending_queue_cleanup.age_for_collection_secs));
    routines.push_back(std::make_unique<RepackRetrieveInactiveMountPendingQueueRoutine>(
      m_lc,
      m_resources.getCatalogue(),
      m_resources.getSchedDb(),
      m_config.routines.repack_pending_queue_cleanup.batch_size,
      m_config.routines.repack_pending_queue_cleanup.age_for_collection_secs));
  }
  // Add Scheduler Maintenance Cleanup
  if (m_config.routines.scheduler_maintenance_cleanup.enabled) {
    routines.push_back(std::make_unique<DeleteOldFailedQueuesRoutine>(
      m_lc,
      m_resources.getSchedDb(),
      m_config.routines.scheduler_maintenance_cleanup.batch_size,
      m_config.routines.scheduler_maintenance_cleanup.age_for_deletion_secs));
    routines.push_back(std::make_unique<CleanMountLastFetchTimeRoutine>(
      m_lc,
      m_resources.getSchedDb(),
      m_config.routines.scheduler_maintenance_cleanup.batch_size,
      m_config.routines.scheduler_maintenance_cleanup.age_for_deletion_secs));
    // Disk system sleep entry cleanup rides along with the rest of the scheduler maintenance
    // cleanup above rather than getting its own routine group and deployment. Mount decisions
    // already ignore an entry once its own SLEEP_TIME/LAST_UPDATE_TIME has elapsed, independently
    // of whether the row has been deleted yet, so running this as infrequently as the daily
    // scheduler_maintenance_cleanup cadence does not affect the disk system sleep (backpressure)
    // functionality at all — it only affects how long the (at most one per disk system) expired
    // rows linger in the table.
    routines.push_back(std::make_unique<DeleteExpiredDiskSystemSleepEntriesRoutine>(m_lc, m_resources.getSchedDb()));
  }
#endif

  m_lc.log(log::INFO, "In RoutineRunnerFactory::create(): Routines created");
  return std::make_unique<RoutineRunner>(m_config.routines, std::move(routines));
}

}  // namespace cta::maintd
