/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeSessionReporter.hpp"

#include "TapedProxyMock.hpp"
#include "common/log/DummyLogger.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace unitTests {

class TapeSessionReporterTest : public ::testing::Test {
protected:
  cta::log::DummyLogger m_logger {"dummy", "unitTest"};
  cta::log::LogContext m_lc {m_logger};
  ::testing::StrictMock<cta::tape::daemon::TapedProxyMock> m_proxy;
};

TEST_F(TapeSessionReporterTest, ReleasesReportsWithoutStartingWorker) {
  cta::tape::daemon::TapeSessionReporter reporter(m_proxy, m_lc);

  // An aborted mount can leave scheduling reports queued without starting a worker.
  // The strict mock requires that destruction discards them without reporting state.
  for (int i = 0; i < 3; ++i) {
    reporter.reportState(cta::tape::session::SessionState::Scheduling, cta::tape::session::SessionType::Undetermined);
  }
}

TEST_F(TapeSessionReporterTest, DeliversReportsThroughWorker) {
  cta::tape::daemon::TapeSessionReporter reporter(m_proxy, m_lc);
  EXPECT_CALL(
    m_proxy,
    reportState(cta::tape::session::SessionState::Scheduling, cta::tape::session::SessionType::Undetermined, ""));

  reporter.reportState(cta::tape::session::SessionState::Scheduling, cta::tape::session::SessionType::Undetermined);
  reporter.startThreads();
  reporter.finish();
  reporter.waitThreads();
}

TEST_F(TapeSessionReporterTest, BailoutDeliversQueuedReports) {
  cta::tape::daemon::TapeSessionReporter reporter(m_proxy, m_lc);
  EXPECT_CALL(
    m_proxy,
    reportState(cta::tape::session::SessionState::Scheduling, cta::tape::session::SessionType::Undetermined, ""));

  reporter.reportState(cta::tape::session::SessionState::Scheduling, cta::tape::session::SessionType::Undetermined);
  reporter.bailout();
}

}  // namespace unitTests
