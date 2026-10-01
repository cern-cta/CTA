/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SchedulerContext.hpp"

#include "common/utils/utils.hpp"

namespace cta::tape::daemon {

SchedulerContext::SchedulerContext(const TapedConfig& config, catalogue::Catalogue& catalogue, log::Logger& log)
    : m_config(config),
      m_catalogue(catalogue),
      m_lc(log) {
  initialise();
}

void SchedulerContext::retire() {
#ifndef CTA_PGSCHED
  // Nonempty object-store agents remain available for garbage collection.
  initialise();
#endif
}

void SchedulerContext::initialise() {
  m_lc.log(log::INFO, "Initialising Scheduler");
#ifndef CTA_PGSCHED
  // Nonempty agents must remain registered so garbage collection can recover abandoned jobs.
  auto schedDbInit =
    std::make_unique<SchedulerDBInit_t>("Taped",
                                        utils::readSingleLineConfigFile(m_config.scheduler.config_file),
                                        m_lc.logger(),
                                        true);
#else
  auto schedDbInit =
    std::make_unique<SchedulerDBInit_t>("Taped",
                                        utils::readSingleLineConfigFile(m_config.scheduler.config_file),
                                        m_config.scheduler.number_of_connections,
                                        m_lc.logger());
#endif
  auto schedDb = schedDbInit->getSchedDB(m_catalogue, m_lc.logger());
  SchedulerDatabase::StatisticsCacheConfig statisticsCacheConfig;
  statisticsCacheConfig.tapeCacheMaxAgeSecs = m_config.scheduler.tape_cache_max_age_secs;
  statisticsCacheConfig.retrieveQueueCacheMaxAgeSecs = m_config.scheduler.retrieve_queue_cache_max_age_secs;
  schedDb->setStatisticsCacheConfig(statisticsCacheConfig);
  auto scheduler = std::make_unique<Scheduler>(m_catalogue,
                                               *schedDb,
                                               m_config.scheduler.backend_name,
                                               m_config.mounts.minimum_queued_files,
                                               m_config.mounts.minimum_queued_bytes);

  // Publish only a fully constructed replacement. Locals destroy the old context in reverse order.
  m_schedDbInit.swap(schedDbInit);
  m_schedDb.swap(schedDb);
  m_scheduler.swap(scheduler);
  m_lc.log(log::INFO, "Scheduler initialised successfully");
}

}  // namespace cta::tape::daemon
