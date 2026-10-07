/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/DriveStatus.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Param.hpp"
#include "common/process/threading/Thread.hpp"
#include "scheduler/TapeMount.hpp"
#include "taped/session/TapeSessionTracker.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>

namespace cta::tape::daemon {

/// Define catalogue phase mapping and publish tracker statistics and mount metadata.
/// Phase callbacks publish synchronously on transitions; only statistics reporting is periodic.
class TapeSessionReporter : private cta::threading::Thread {
public:
  /// Map a session phase for synchronous publication; local-only phases return no catalogue status.
  static std::optional<common::dataStructures::DriveStatus> driveStatusForSessionState(session::TapeSessionState state);

  /// @brief Create a periodic reporter using a borrowed tracker and mount.
  ///
  /// The tracker and mount must outlive reporting and the worker thread must be joined before destruction.
  ///
  /// @param tracker Session state and statistics to publish.
  /// @param mount Borrowed scheduler mount used for metadata and statistics publication.
  /// @param lc Log context copied for reporter output.
  /// @param reportPeriod Interval between reports, clamped to at least one millisecond.
  /// @param stuckPeriod Block-inactivity threshold and minimum interval between warnings, clamped to one millisecond.
  TapeSessionReporter(TapeSessionTracker& tracker,
                      cta::TapeMount& mount,
                      const cta::log::LogContext& lc,
                      std::chrono::milliseconds reportPeriod,
                      std::chrono::milliseconds stuckPeriod);

  /// Start the background reporting thread.
  void startThreads();

  /// @brief Request the reporting thread to stop and wake its wait loop.
  ///
  /// Call waitThreads() to join the thread.
  void finish();

  /// @brief Join the reporting thread.
  ///
  /// @pre finish() has been called to request shutdown.
  void waitThreads();

  /// @brief Log current statistics and publish tape-transfer statistics to the mount.
  ///
  /// Do not call concurrently with the reporting thread; publication failures propagate.
  void reportNow();

  /// @brief Publish final statistics and outcome when the tracker is Finalizing or Finished.
  ///
  /// Call once after joining transfer workers and the periodic reporter; other phases are ignored.
  /// Statistics-publication failures are counted before logging the final outcome.
  void reportSessionFinished();

private:
  TapeSessionTracker& m_tracker;
  cta::TapeMount& m_mount;
  cta::log::LogContext m_lc;
  const std::chrono::milliseconds m_reportPeriod;
  const std::chrono::milliseconds m_stuckPeriod;

  std::mutex m_mutex;
  std::condition_variable m_condition;
  bool m_finishRequested = false;
  /// Time of the last inactivity warning, used to limit repeated warnings.
  std::chrono::steady_clock::time_point m_lastStuckReport;

  /// Publish periodic statistics and inactivity warnings until finish() is requested.
  void run() override;

  /// Warn about an active tape file with no recent block movement, limiting repeated warnings.
  void reportStuckFileIfNeeded();

  /// @brief Log a statistics snapshot with current mount metadata, progress and error counters.
  ///
  /// @param sessionFinished Select a final outcome event instead of an in-progress statistics log.
  /// @param stats Statistics snapshot to include in the log.
  void logStats(bool sessionFinished, const TapeSessionStats& stats);
};

}  // namespace cta::tape::daemon
