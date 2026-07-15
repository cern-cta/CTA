/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "mountdecision/MountDecisionDB.hpp"
#include "scheduler/rdbms/ConnProvider.hpp"

#include <memory>
#include <string>
#include <string_view>

namespace cta {
class Scheduler;
class SchedulerDatabase;
class TapeMount;
}  // namespace cta

namespace cta::mountdecision {

struct ReservedTapeMount {
  uint64_t candidateId = 0;
  std::unique_ptr<TapeMount> mount;
};

/**
 * High-level Mount Decision component interface.
 *
 * This class owns the Mount Decision backend wrapper and uses the supplied
 * connection provider to access the scheduler DB.
 */
class MountDecision final {
public:
  MountDecision(ConnProvider& connectionProvider, SchedulerDatabase& schedulerDb, log::LogContext& lc);

  bool isAvailable() const;

  bool incrementCounter(const std::string& key, log::LogContext& lc, std::string_view operation);

  bool refreshMountCandidates(const std::string& owner, Scheduler& scheduler, log::LogContext& lc);

  std::optional<ReservedTapeMount> getNextMount(const std::string& logicalLibraryName,
                                                const std::string& driveName,
                                                Scheduler& scheduler,
                                                log::LogContext& lc,
                                                uint64_t timeout_us);

  void releaseMountCandidate(uint64_t candidateId,
                             const std::string& host,
                             const std::string& drive,
                             log::LogContext& lc) noexcept;

  void heartbeatMountCandidate(uint64_t candidateId, const std::string& host, const std::string& drive) noexcept;

private:
  std::unique_ptr<MountDecisionDB> m_db;
  SchedulerDatabase& m_schedulerDb;
};

}  // namespace cta::mountdecision
