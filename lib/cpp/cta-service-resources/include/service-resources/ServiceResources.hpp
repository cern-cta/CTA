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
#include "service-resources/config/FilesConfig.hpp"
#include "service-resources/config/SchedulerConfig.hpp"

#include <memory>

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

namespace cta::service_resources {

/**
 * @brief Resources shared by the CTA services (admin and workflow APIs): owns the Catalogue,
 * the Scheduler DB and the Scheduler. Built once at start-up from the typed configuration.
 *
 * The logger is not owned here: it is provided by the runtime and passed in.
 */
class ServiceResources final {
public:
  /**
   * @param catalogueConfig     Catalogue location (runtime)
   * @param schedulerConfig     Scheduler DB location and cache settings (runtime)
   * @param catalogueOptions    Catalogue connection pool size (service-specific)
   * @param filesOptions        Missing copies / recycle log options (service-specific)
   * @param schedulerOptions    Scheduler DB thread pool options (service-specific, objectstore only)
   * @param log                 Logger provided by the outer scope
   * @throws cta::exception::UserError / cta::exception::Exception on invalid configuration or connection failure
   */
  ServiceResources(const runtime::CatalogueConfig& catalogueConfig,
                   const runtime::SchedulerConfig& schedulerConfig,
                   const CatalogueConfig& catalogueOptions,
                   const FilesConfig& filesOptions,
#ifndef CTA_PGSCHED
                   const SchedulerConfig& schedulerOptions,
#endif
                   log::Logger& log);

  ServiceResources(const ServiceResources&) = delete;
  ServiceResources& operator=(const ServiceResources&) = delete;

  /** @brief Logger provided by the outer scope */
  log::Logger& getLogger() const { return m_log; }

  /** @brief Log context bound to the logger */
  log::LogContext getLogContext() const { return log::LogContext(m_log); }

  /** @brief Connection string of the Catalogue */
  const std::string& getCatalogueConnString() const { return m_catalogueConnString; }

  /** @brief Catalogue shared by the RPC services */
  catalogue::Catalogue& getCatalogue() const { return *m_catalogue; }

  /** @brief Scheduler DB shared by the RPC services */
  SchedulerDB_t& getSchedDb() const { return *m_schedDb; }

  /** @brief Scheduler shared by the RPC services */
  Scheduler& getScheduler() const { return *m_scheduler; }

  /** @brief Minimum age (in seconds) of missing tape file copies before they are reported */
  uint64_t getMissingFileCopiesMinAgeSecs() const { return m_missingFileCopiesMinAgeSecs; }

  /** @brief Minimum quarantine period (in seconds) before a tape can be reclaimed */
  uint64_t getRecycleLogQuarantineSecs() const { return m_recycleLogQuarantineSecs; }

private:
  log::Logger& m_log;                                 //!< Logger provided by the outer scope (not owned)
  uint64_t m_missingFileCopiesMinAgeSecs;             //!< Min age (s) of missing tape file copies before reporting
  uint64_t m_recycleLogQuarantineSecs;                //!< Min quarantine period (s) before a tape can be reclaimed
  std::string m_catalogueConnString;                  //!< Connection string of the Catalogue
  std::unique_ptr<catalogue::Catalogue> m_catalogue;  //!< Catalogue, created through the catalogue factory
  std::unique_ptr<SchedulerDBInit_t> m_schedDbInit;   //!< Scheduler DB initialiser; must outlive m_schedDb
  std::unique_ptr<SchedulerDB_t> m_schedDb;           //!< Scheduler DB, obtained from m_schedDbInit
  std::unique_ptr<Scheduler> m_scheduler;             //!< Scheduler on top of m_catalogue and m_schedDb
};

}  // namespace cta::service_resources
