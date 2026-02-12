/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "RoutineRunner.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/config/Config.hpp"
#include "common/log/LogContext.hpp"
#include "scheduler/Scheduler.hpp"

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

namespace cta::maintd {

/**
 * Responsible for create a RoutineRunner with a specific set of registered routines based on the provided config.
 */
class RoutineRunnerFactory final {
public:
  RoutineRunnerFactory(const MaintdConfig& config, cta::log::LogContext& lc);
  std::unique_ptr<RoutineRunner> create();

private:
  const MaintdConfig& m_config;
  cta::log::LogContext& m_lc;

  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<cta::SchedulerDBInit_t> m_schedDbInit;
  std::unique_ptr<cta::SchedulerDB_t> m_schedDb;
  std::unique_ptr<cta::Scheduler> m_scheduler;
};

}  // namespace cta::maintd
