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

// Keeps scheduler construction and backend lifetimes in one place.
// Revisit this wrapper when the objectstore backend is removed.

namespace cta::tape::daemon {

/// @brief Manage the scheduler and the database objects it depends on.
///
/// Either create and own these objects or use a scheduler owned by the caller.
/// The configuration, catalogue and logger must outlive this context.
class SchedulerContext final {
public:
  /// @brief Construct a scheduler and its backend using the supplied configuration.
  ///
  /// The caller retains ownership of the configuration, catalogue and logger.
  SchedulerContext(const TapedConfig& config, catalogue::Catalogue& catalogue, log::Logger& log);

  /// @brief Borrow a scheduler and its catalogue without taking ownership.
  ///
  /// The configuration, logger, scheduler, its catalogue and its backend must outlive this context.
  SchedulerContext(const TapedConfig& config, log::Logger& log, Scheduler& scheduler);

  /// @brief Return the active scheduler.
  ///
  /// The reference is invalidated when renewObjectStoreAgent() replaces an owned objectstore backend.
  Scheduler& scheduler() { return *m_scheduler; }

  /// The borrowed catalogue remains valid across scheduler renewal.
  catalogue::Catalogue& catalogue() { return m_catalogue; }

  /// @brief Recreate the owned objectstore scheduler and backend with a new job-ownership agent.
  ///
  /// Leave the old agent available for garbage collection to recover abandoned jobs.
  /// Does nothing for PostgreSQL or a scheduler owned by the caller.
  void renewObjectStoreAgent();

private:
  /// @brief Construct a complete scheduler/backend pair before replacing the current pair.
  ///
  /// If construction throws, keep the existing scheduler and backend.
  void initialise();

  const TapedConfig& m_config;
  catalogue::Catalogue& m_catalogue;
  log::LogContext m_lc;
  std::unique_ptr<SchedulerDBInit_t> m_schedDbInit;
  std::unique_ptr<SchedulerDB_t> m_schedDb;
  std::unique_ptr<Scheduler> m_ownedScheduler;
  /// Non-owning access to either m_ownedScheduler or the externally supplied scheduler.
  Scheduler* m_scheduler = nullptr;
};

}  // namespace cta::tape::daemon
