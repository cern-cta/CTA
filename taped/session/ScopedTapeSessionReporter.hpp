/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "WorkerFailureHandling.hpp"

namespace cta::tape::daemon {

/// Stop and join the reporting thread before the session state it references is destroyed.
/// Exit the process if safe reporter shutdown cannot be established.
template<typename Reporter>
class ScopedTapeSessionReporter {
public:
  /// @brief Borrow a reporter without starting its thread.
  ///
  /// @param reporter Borrowed reporter whose thread is managed by this guard.
  /// @param lc Session context retained for fatal diagnostics.
  ScopedTapeSessionReporter(Reporter& reporter, log::LogContext& lc) : m_reporter(reporter), m_lc(lc) {}

  /// Disallow copying the owner of a reporter thread.
  ScopedTapeSessionReporter(const ScopedTapeSessionReporter&) = delete;

  /// Disallow assigning ownership of a reporter thread.
  ScopedTapeSessionReporter& operator=(const ScopedTapeSessionReporter&) = delete;

  /// Stop and join before borrowed state is destroyed; exit immediately if termination is unproven.
  ~ScopedTapeSessionReporter() noexcept { finish(); }

  /// Start reporting and record that this guard must join the thread.
  void start() {
    try {
      m_reporter.startThreads();
    } catch (...) {
      exitOnReporterFailure();
    }
    m_started = true;
  }

  /// Request shutdown and join the reporter if it was started.
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
