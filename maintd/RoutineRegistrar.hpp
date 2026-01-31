/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "MaintdConfig.hpp"
#include "RoutineRunner.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/log/LogContext.hpp"
#include "scheduler/Scheduler.hpp"

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

namespace cta::maintd {

/**
 * Responsible for registring a set of routines in a RoutineRunner based on the provided config.
 * TODO: think of a cleaner way to do this
 */
class RoutineRegistrar {
public:
  RoutineRegistrar(const MaintdConfig& config, cta::log::LogContext& lc);
  void registerRoutines(RoutineRunner& routineRunner);

private:
  const MaintdConfig& m_config;
  cta::log::LogContext& m_lc;

  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<cta::SchedulerDBInit_t> m_schedDbInit;
  std::unique_ptr<cta::SchedulerDB_t> m_schedDb;
  std::unique_ptr<cta::Scheduler> m_scheduler;
};

}  // namespace cta::maintd
