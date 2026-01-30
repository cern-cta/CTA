/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "IRoutine.hpp"
#include "common/log/LogContext.hpp"

#include <vector>

namespace cta::maintd {

/**
 * Responsible for running routines. It does so as follows:
 * 1. for all registered routines, execute the routine (synchronously)
 * 2. sleep
 * 3. Go to 1.
 */
class RoutineRunner {
public:
  explicit RoutineRunner(int64_t sleepIntervalSecs, int64_t maxCycleDurationSecs);

  ~RoutineRunner() = default;

  void registerRoutine(std::unique_ptr<IRoutine> routine);

  void stop();

  /**
   * Periodically executes all registered routines.
   */
  void run(cta::log::LogContext& lc);

  bool isLive();

  bool isReady();

private:
  void safeRunRoutine(IRoutine& routine, cta::log::LogContext& lc);

  std::vector<std::unique_ptr<IRoutine>> m_routines;

  const int64_t m_sleepIntervalSecs;
  const int64_t m_maxCycleDurationSecs;

  std::atomic<bool> m_running = false;

  std::atomic<int64_t> m_executionStartTime {0};
  std::atomic<int64_t> m_sleepStartTime {0};
};

}  // namespace cta::maintd
