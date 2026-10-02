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

/// @brief Own a scheduler and backend, or borrow an externally managed scheduler.
///
/// The configuration, catalogue and logger must outlive this context.
class SchedulerContext final {
public:
  /// @brief Construct a scheduler and its backend using the supplied configuration.
  ///
  /// The configuration, catalogue and logger are borrowed.
  SchedulerContext(const TapedConfig& config, catalogue::Catalogue& catalogue, log::Logger& log);

  /// @brief Borrow a scheduler and its catalogue without taking ownership.
  ///
  /// The supplied dependencies and backend must outlive this context.
  SchedulerContext(const TapedConfig& config, log::Logger& log, Scheduler& scheduler);

  /// @brief Return the active scheduler.
  ///
  /// The reference is invalidated when renewObjectStoreAgent() replaces an owned object-store backend.
  Scheduler& scheduler() { return *m_scheduler; }

  /// @brief Replace an owned object-store scheduler and backend with a fresh agent.
  ///
  /// Does nothing for PostgreSQL or a borrowed scheduler.
  void renewObjectStoreAgent();

private:
  /// @brief Construct a complete scheduler/backend pair before replacing the current pair.
  ///
  /// Construction failures propagate without replacing the existing pair.
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
