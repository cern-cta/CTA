/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "IRoutine.hpp"
#include "MaintdConfig.hpp"
#include "common/log/LogContext.hpp"

#include <vector>

namespace cta::maintd {

/**
 * Responsible for running routines. It does so as follows:
 * 1. for all registered routines, execute the routine (synchronously)
 * 2. sleep
 * 3. Go to 1.
 */
class RoutineRunner final {
public:
  RoutineRunner(const RoutinesConfig& routinesConfig, std::vector<std::unique_ptr<IRoutine>> routines);

  ~RoutineRunner() = default;

  void registerRoutine(std::unique_ptr<IRoutine> routine);

  void stop();

  /**
   * Periodically executes all registered routines.
   */
  void run(cta::log::LogContext& lc);

  bool isLive() const;

  bool isReady() const;

private:
  void safeRunRoutine(IRoutine& routine, cta::log::LogContext& lc) const;

  const RoutinesConfig& m_config;
  const std::vector<std::unique_ptr<IRoutine>> m_routines;

  std::atomic<bool> m_running = false;

  std::atomic<int64_t> m_executionStartTime {0};
  std::atomic<int64_t> m_sleepStartTime {0};
};

}  // namespace cta::maintd
