/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/server/ServiceContext.hpp"

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/Login.hpp"

namespace cta::grpc_server {

namespace {
// Number of connections dedicated to archive file listing (fixed, as before the move to the typed config)
constexpr uint64_t NB_ARCHIVE_FILE_LISTING_CONNS = 2;
}  // namespace

ServiceContext::ServiceContext(const runtime::CatalogueConfig& catalogueConfig,
                               const runtime::SchedulerConfig& schedulerConfig,
                               const CatalogueConfig& catalogueOptions,
                               const FilesConfig& filesOptions,
#ifndef CTA_PGSCHED
                               const SchedulerConfig& schedulerOptions,
#endif
                               log::Logger& log)
    : m_log(log),
      m_missingFileCopiesMinAgeSecs(filesOptions.missing_file_copies_min_age_secs),
      m_recycleLogQuarantineSecs(filesOptions.recycle_log_quarantine_secs) {
  log::LogContext lc(m_log);
  lc.log(log::INFO, "In ServiceContext::ServiceContext(): Initialising Catalogue");

  // Initialise the Catalogue
  const rdbms::Login catalogueLogin = rdbms::Login::parseFile(catalogueConfig.config_file);
  auto catalogueFactory = catalogue::CatalogueFactoryFactory::create(m_log,
                                                                     catalogueLogin,
                                                                     catalogueOptions.number_of_connections,
                                                                     NB_ARCHIVE_FILE_LISTING_CONNS);
  m_catalogue = catalogueFactory->create();
  m_catalogueConnString = catalogueLogin.connectionString;
  try {
    m_catalogue->Schema()->ping();
  } catch (cta::exception::Exception& ex) {
    lc.log(log::CRIT, ex.getMessageValue());
    throw;
  }

  lc.log(log::INFO, "In ServiceContext::ServiceContext(): Initialising Scheduler DB");

  // Initialise the Scheduler DB
#ifndef CTA_PGSCHED
  m_schedDbInit = std::make_unique<SchedulerDBInit_t>("Frontend",
                                                      utils::readSingleLineConfigFile(schedulerConfig.config_file),
                                                      m_log);
#else
  m_schedDbInit = std::make_unique<SchedulerDBInit_t>("Frontend",
                                                      utils::readSingleLineConfigFile(schedulerConfig.config_file),
                                                      schedulerConfig.number_of_connections,
                                                      m_log);
#endif
  m_schedDb = m_schedDbInit->getSchedDB(*m_catalogue, m_log);

  // Set Scheduler DB cache timeouts
  SchedulerDatabase::StatisticsCacheConfig statisticsCacheConfig;
  statisticsCacheConfig.tapeCacheMaxAgeSecs = schedulerConfig.tape_cache_max_age_secs;
  statisticsCacheConfig.retrieveQueueCacheMaxAgeSecs = schedulerConfig.retrieve_queue_cache_max_age_secs;
  m_schedDb->setStatisticsCacheConfig(statisticsCacheConfig);

#ifndef CTA_PGSCHED
  // [[OStoreDB specific]] thread pool settings, to be removed together with OStoreDB
  std::optional<int> threadStackSize;
  if (schedulerOptions.thread_stack_size_mb.has_value()) {
    threadStackSize = static_cast<int>(*schedulerOptions.thread_stack_size_mb);
  }
  m_schedDb->initConfig(static_cast<int>(schedulerOptions.number_of_threads), threadStackSize);
#endif

  // Initialise the Scheduler
  m_scheduler = std::make_unique<Scheduler>(*m_catalogue, *m_schedDb, schedulerConfig.backend_name);

  lc.log(log::INFO, "In ServiceContext::ServiceContext(): Scheduler and Catalogue initialised");
}

}  // namespace cta::grpc_server
