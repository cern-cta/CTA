/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "common/log/Param.hpp"
#include "common/process/threading/Thread.hpp"
#include "scheduler/TapeMount.hpp"
#include "taped/session/TapeSessionTracker.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>

namespace cta::tape::daemon {

class TapeSessionReporter : private cta::threading::Thread {
public:
  // The tracker must reference a live mount until all reporting has finished.
  TapeSessionReporter(TapeSessionTracker& tracker,
                      const cta::log::LogContext& lc,
                      std::chrono::milliseconds reportPeriod,
                      std::chrono::milliseconds stuckPeriod);

  void startThreads();
  void finish();
  void waitThreads();

  /** Immediately report the current tracker contents. */
  void reportNow();

private:
  void run() override;
  void reportStuckFileIfNeeded();
  void reportSessionFinished();
  void logStats(bool sessionFinished);

  TapeSessionTracker& m_tracker;
  cta::log::LogContext m_lc;
  const std::chrono::milliseconds m_reportPeriod;
  const std::chrono::milliseconds m_stuckPeriod;

  std::mutex m_mutex;
  std::condition_variable m_condition;
  bool m_finishRequested = false;
  std::chrono::steady_clock::time_point m_lastStuckReport;
};

}  // namespace cta::tape::daemon
