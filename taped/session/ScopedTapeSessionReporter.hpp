/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "WorkerFailureHandling.hpp"

namespace cta::tape::daemon {

/// @brief Stop and join the reporting thread before the session state it references is destroyed.
/// Exit the process if safe reporter shutdown cannot be established.
template<typename Reporter>
class ScopedTapeSessionReporter {
public:
  /// Borrow a reporter and diagnostic context that outlive this guard; do not start reporting yet.
  ScopedTapeSessionReporter(Reporter& reporter, log::LogContext& lc) : m_reporter(reporter), m_lc(lc) {}

  /// Disallow copying the owner of a reporter thread.
  ScopedTapeSessionReporter(const ScopedTapeSessionReporter&) = delete;

  /// Disallow assigning ownership of a reporter thread.
  ScopedTapeSessionReporter& operator=(const ScopedTapeSessionReporter&) = delete;

  /// Stop and join before borrowed state is destroyed; exit immediately if termination is unproven.
  ~ScopedTapeSessionReporter() noexcept { finish(); }

  /// @brief Start reporting and arrange for this guard to join the thread.
  ///
  /// Call once; a startup exception exits the process without unwinding.
  void start() {
    try {
      m_reporter.startThreads();
    } catch (...) {
      exitOnReporterFailure();
    }
    m_started = true;
  }

  /// @brief Stop and join the reporter if started; subsequent calls do nothing.
  ///
  /// Shutdown failure exits the process without unwinding.
  void finish() noexcept {
    if (!m_started) {
      return;
    }
    try {
      m_reporter.finish();
      m_reporter.waitThreads();
    } catch (...) {
      exitOnReporterFailure();
    }
    m_started = false;
  }

private:
  /// Preserve borrowed session state when reporter termination cannot be established.
  [[noreturn]] void exitOnReporterFailure() noexcept {
    exitOnUnsafeWorkerFailure(
      m_lc,
      "Reporter termination could not be established. Exiting immediately to protect borrowed session state.");
  }

  Reporter& m_reporter;
  log::LogContext& m_lc;
  bool m_started = false;
};

}  // namespace cta::tape::daemon
