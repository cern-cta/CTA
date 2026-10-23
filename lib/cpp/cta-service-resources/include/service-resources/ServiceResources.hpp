/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/Catalogue.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "runtime/config/CommonConfig.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "service-resources/config/CatalogueConfig.hpp"
#include "service-resources/config/SchedulerConfig.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

namespace cta::service_resources {

/// @brief Application-specific connection settings of the service resources.
struct ConnectionOptions final {
  /// Size of the Catalogue connection pool
  uint64_t catalogueConns = 1;
  /// Size of the Catalogue archive file listing connection pool
  uint64_t archiveFileListingConns = 1;
#ifndef CTA_PGSCHED
  /// Objectstore thread pool size (no thread pool if not set)
  std::optional<uint32_t> osThreadPoolSize = std::nullopt;
  /// Objectstore thread stack size (MB)
  std::optional<uint32_t> osThreadStackSizeMb = std::nullopt;
#endif
};

/// @brief Connection settings of the services reading `[catalogue.client]` and `[scheduler.client]`
/// (admin and workflow APIs).
ConnectionOptions makeConnectionOptions(const ServiceCatalogueConfig& catalogueConfig,
                                        const ServiceSchedulerConfig& schedulerConfig);

/// @brief Resources shared by the CTA services: owns the Catalogue, the Scheduler DB and the Scheduler.
/// Built once at start-up from the typed configuration.
///
/// The logger is not owned here: it is provided by the runtime and passed in.
class ServiceResources final {
public:
  /// @param processName         Name of the process, used by the Scheduler DB (e.g. objectstore agent name)
  /// @param catalogueConfig     Catalogue location (`[catalogue]`); application-specific client options are ignored
  /// @param schedulerConfig     Scheduler DB location and cache settings (`[scheduler]`); application-specific client
  ///                            options are ignored
  /// @param connections         Application-specific connection settings
  /// @param log                 Logger provided by the outer scope
  /// @throws cta::exception::UserError / cta::exception::Exception on invalid configuration or connection failure
  template<typename CatalogueClientConfig, typename SchedulerClientConfig>
  ServiceResources(std::string_view processName,
                   const runtime::BasicCatalogueConfig<CatalogueClientConfig>& catalogueConfig,
                   const runtime::BasicSchedulerConfig<SchedulerClientConfig>& schedulerConfig,
                   const ConnectionOptions& connections,
                   log::Logger& log)
      : ServiceResources(
          Params {
            .processName = std::string(processName),
            .catalogueConfigFile = catalogueConfig.config_file,
            .schedulerConfigFile = schedulerConfig.config_file,
            .schedulerBackendName = schedulerConfig.backend_name,
            .statisticsCacheConfig = {.tapeCacheMaxAgeSecs = schedulerConfig.tape_cache_max_age_secs,
                                      .retrieveQueueCacheMaxAgeSecs =
                                        schedulerConfig.retrieve_queue_cache_max_age_secs},
#ifdef CTA_PGSCHED
            .schedulerConns = schedulerConfig.number_of_connections,
#endif
            .connections = connections
  },
          log) {
  }

  ServiceResources(const ServiceResources&) = delete;
  ServiceResources& operator=(const ServiceResources&) = delete;

  /// @brief Logger provided by the outer scope
  log::Logger& getLogger() const { return m_log; }

  /// @brief Log context bound to the logger
  log::LogContext getLogContext() const { return log::LogContext(m_log); }

  /// @brief Connection string of the Catalogue
  const std::string& getCatalogueConnString() const { return m_catalogueConnString; }

  /// @brief Catalogue shared by the services
  catalogue::Catalogue& getCatalogue() const { return *m_catalogue; }

#ifndef CTA_PGSCHED
  /// @brief [[OStoreDB specific]] Scheduler DB initialiser, giving access to the objectstore backend and agent
  SchedulerDBInit_t& getSchedDbInit() const { return *m_schedDbInit; }
#endif

  /// @brief Scheduler DB shared by the services
  SchedulerDB_t& getSchedDb() const { return *m_schedDb; }

  /// @brief Scheduler shared by the services
  Scheduler& getScheduler() const { return *m_scheduler; }

private:
  /// Configuration-independent construction parameters
  struct Params final {
    std::string processName;
    std::string catalogueConfigFile;
    std::string schedulerConfigFile;
    std::string schedulerBackendName;
    SchedulerDatabase::StatisticsCacheConfig statisticsCacheConfig;
#ifdef CTA_PGSCHED
    uint64_t schedulerConns;
#endif
    ConnectionOptions connections;
  };

  ServiceResources(const Params& params, log::Logger& log);

  /// Logger provided by the outer scope (not owned)
  log::Logger& m_log;
  /// Connection string of the Catalogue
  std::string m_catalogueConnString;
  /// Catalogue, created through the catalogue factory
  std::unique_ptr<catalogue::Catalogue> m_catalogue;
  /// Scheduler DB initialiser; must outlive m_schedDb
  std::unique_ptr<SchedulerDBInit_t> m_schedDbInit;
  /// Scheduler DB, obtained from m_schedDbInit
  std::unique_ptr<SchedulerDB_t> m_schedDb;
  /// Scheduler on top of m_catalogue and m_schedDb
  std::unique_ptr<Scheduler> m_scheduler;
};

}  // namespace cta::service_resources
