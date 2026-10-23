/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "service-resources/ServiceResources.hpp"

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/Login.hpp"

namespace cta::service_resources {

namespace {
/// Number of connections dedicated to archive file listing by the admin and workflow APIs
constexpr uint64_t NB_ARCHIVE_FILE_LISTING_CONNS = 2;
}  // namespace

ConnectionOptions makeConnectionOptions(const ServiceCatalogueConfig& catalogueConfig,
                                        const ServiceSchedulerConfig& schedulerConfig) {
  ConnectionOptions connections;
  connections.catalogueConns = catalogueConfig.client.number_of_connections;
  connections.archiveFileListingConns = NB_ARCHIVE_FILE_LISTING_CONNS;
#ifndef CTA_PGSCHED
  connections.osThreadPoolSize = schedulerConfig.client.number_of_threads;
  connections.osThreadStackSizeMb = schedulerConfig.client.thread_stack_size_mb;
#endif
  return connections;
}

ServiceResources::ServiceResources(const Params& params, log::Logger& log) : m_log(log) {
  log::LogContext lc(m_log);
  lc.log(log::INFO, "In ServiceResources::ServiceResources(): Initialising Catalogue");

  // Initialise the Catalogue
  const rdbms::Login catalogueLogin = rdbms::Login::parseFile(params.catalogueConfigFile);
  auto catalogueFactory = catalogue::CatalogueFactoryFactory::create(m_log,
                                                                     catalogueLogin,
                                                                     params.connections.catalogueConns,
                                                                     params.connections.archiveFileListingConns);
  m_catalogue = catalogueFactory->create();
  m_catalogueConnString = catalogueLogin.connectionString;
  try {
    m_catalogue->Schema()->ping();
  } catch (cta::exception::Exception& ex) {
    lc.log(log::CRIT, ex.getMessageValue());
    throw;
  }

  lc.log(log::INFO, "In ServiceResources::ServiceResources(): Initialising Scheduler DB");

  // Initialise the Scheduler DB
#ifndef CTA_PGSCHED
  m_schedDbInit = std::make_unique<SchedulerDBInit_t>(params.processName,
                                                      utils::readSingleLineConfigFile(params.schedulerConfigFile),
                                                      m_log);
#else
  m_schedDbInit = std::make_unique<SchedulerDBInit_t>(params.processName,
                                                      utils::readSingleLineConfigFile(params.schedulerConfigFile),
                                                      params.schedulerConns,
                                                      m_log);
#endif
  m_schedDb = m_schedDbInit->getSchedDB(*m_catalogue, m_log);

  // Set Scheduler DB cache timeouts
  m_schedDb->setStatisticsCacheConfig(params.statisticsCacheConfig);

#ifndef CTA_PGSCHED
  // [[OStoreDB specific]] thread pool settings, to be removed together with OStoreDB
  if (params.connections.osThreadPoolSize.has_value()) {
    std::optional<int> threadStackSize;
    if (params.connections.osThreadStackSizeMb.has_value()) {
      threadStackSize = static_cast<int>(*params.connections.osThreadStackSizeMb);
    }
    m_schedDb->initConfig(static_cast<int>(*params.connections.osThreadPoolSize), threadStackSize);
  }
#endif

  // Initialise the Scheduler
  m_scheduler = std::make_unique<Scheduler>(*m_catalogue, *m_schedDb, params.schedulerBackendName);

  lc.log(log::INFO, "In ServiceResources::ServiceResources(): Scheduler and Catalogue initialised");
}

}  // namespace cta::service_resources
