/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "TapedConfig.hpp"
#include "common/log/LogContext.hpp"
#include "scheduler/Scheduler.hpp"

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

#include <memory>

namespace cta::tape::daemon {

// Owns a scheduler and its backend together, or borrows an externally managed scheduler.
// The catalogue, configuration, and logger must outlive this context.
class SchedulerContext final {
public:
  SchedulerContext(const TapedConfig& config, catalogue::Catalogue& catalogue, log::Logger& log);

  // The supplied scheduler and its backend must outlive the context. retire() leaves them untouched.
  SchedulerContext(const TapedConfig& config, log::Logger& log, Scheduler& scheduler);

  // References are valid only until retire() replaces the object-store backend.
  Scheduler& scheduler() { return *m_scheduler; }

  // Call only after all borrowed mounts and jobs have been destroyed.
  // PostgreSQL has no per-agent ownership to retire.
  void retire();

private:
  void initialise();

  const TapedConfig& m_config;
  catalogue::Catalogue& m_catalogue;
  log::LogContext m_lc;
  std::unique_ptr<SchedulerDBInit_t> m_schedDbInit;
  std::unique_ptr<SchedulerDB_t> m_schedDb;
  std::unique_ptr<Scheduler> m_ownedScheduler;
  Scheduler* m_scheduler = nullptr;
};

}  // namespace cta::tape::daemon
