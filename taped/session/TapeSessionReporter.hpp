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
  /**
   * @brief Create a periodic reporter using a borrowed tracker and its attached mount.
   *
   * The tracker and mount must outlive reporting and the worker thread must be joined before destruction.
   *
   * @param tracker Session state and statistics to publish.
   * @param lc Log context copied for reporter output.
   * @param reportPeriod Interval between reports, clamped to at least one millisecond.
   * @param stuckPeriod Block-inactivity threshold and minimum interval between warnings, clamped to one millisecond.
   */
  TapeSessionReporter(TapeSessionTracker& tracker,
                      const cta::log::LogContext& lc,
                      std::chrono::milliseconds reportPeriod,
                      std::chrono::milliseconds stuckPeriod);

  /**
   * @brief Start the background reporting thread.
   */
  void startThreads();

  /**
   * @brief Request the reporting thread to stop and wake its wait loop.
   *
   * A final event is emitted only if the session owner established Finished.
   * Call waitThreads() to join the thread.
   */
  void finish();

  /**
   * @brief Join the reporting thread after requesting it to stop.
   */
  void waitThreads();

  /**
   * @brief Immediately report the current tracker contents.
   */
  void reportNow();

private:
  TapeSessionTracker& m_tracker;
  cta::log::LogContext m_lc;
  const std::chrono::milliseconds m_reportPeriod;
  const std::chrono::milliseconds m_stuckPeriod;

  std::mutex m_mutex;
  std::condition_variable m_condition;
  bool m_finishRequested = false;
  std::chrono::steady_clock::time_point m_lastStuckReport;

  /**
   * @brief Publish periodic statistics and inactivity warnings until finish() is requested.
   *
   * Then attempt final reporting if the session state permits it.
   */
  void run() override;

  /**
   * @brief Warn about an active tape file with no recent block movement, limiting repeated warnings.
   */
  void reportStuckFileIfNeeded();

  /**
   * @brief Publish final statistics and outcome only when the tracker state is Finished.
   */
  void reportSessionFinished();

  /**
   * @brief Log a statistics snapshot with current mount metadata, progress and error counters.
   *
   * @param sessionFinished Select a final outcome event instead of an in-progress statistics log.
   * @param stats Statistics snapshot to include in the log.
   */
  void logStats(bool sessionFinished, const TapeSessionStats& stats);
};

}  // namespace cta::tape::daemon
