/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RecallReportPacker.hpp"

#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/StringLogger.hpp"
#include "scheduler/testingMocks/MockRetrieveJob.hpp"
#include "scheduler/testingMocks/MockRetrieveMount.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdexcept>

using ::testing::_;
using ::testing::Invoke;

namespace unitTests {

class cta_tape_daemon_RecallReportPackerTest : public ::testing::Test {
protected:
  void SetUp() {}

  void TearDown() {}

};  // class cta_tape_daemon_RecallReportPackerTest

class MockRetrieveJobExternalStats : public cta::MockRetrieveJob {
public:
  MockRetrieveJobExternalStats(cta::RetrieveMount& rm, int& completes, int& failures)
      : MockRetrieveJob(rm),
        completesRef(completes),
        failuresRef(failures) {}

  void asyncSetSuccessful() override { completesRef++; }

  void transferFailed(const std::string& failureReason, cta::log::LogContext&) override { failuresRef++; }

private:
  int& completesRef;
  int& failuresRef;
};

// Observe protocol selection without starting the report worker.
class MockCompletionReportPacker : public cta::tape::daemon::RecallReportPacker {
public:
  using RecallReportPacker::RecallReportPacker;
  MOCK_METHOD(void, reportEndOfSession, (cta::log::LogContext&), (override));
  MOCK_METHOD(void, reportEndOfSessionWithErrors, (const std::string&, cta::log::LogContext&), (override));
};

TEST_F(cta_tape_daemon_RecallReportPackerTest, SelectsCompletionProtocolFromOutcome) {
  using namespace cta::tape::daemon;
  cta::catalogue::DummyCatalogue catalogue;
  cta::MockRetrieveMount mount(catalogue);
  cta::log::StringLogger log("dummy", "completionPolicy", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  TapeSessionTracker tracker;
  MockCompletionReportPacker packer(&mount, lc, tracker);

  const auto check = [&](bool diagnostics) {
    EXPECT_CALL(packer, reportEndOfSession(testing::Ref(lc))).Times(diagnostics ? 0 : 1);
    EXPECT_CALL(packer, reportEndOfSessionWithErrors("End of recall session with error(s)", testing::Ref(lc)))
      .Times(diagnostics ? 1 : 0);
    packer.reportEndOfSessionFromOutcome(tracker.outcomeSnapshot(), lc);
    EXPECT_TRUE(testing::Mock::VerifyAndClearExpectations(&packer));
  };

  check(false);
  for (size_t i = 0; i < static_cast<size_t>(TapeSessionFailure::Count); ++i) {
    SCOPED_TRACE(i);
    tracker.beginTapeSession(cta::tape::session::SessionType::Retrieve);
    const auto failure = static_cast<TapeSessionFailure>(i);
    tracker.recordFailure(failure);
    const bool propagationOnly =
      failure == TapeSessionFailure::UnexpectedSession || failure == TapeSessionFailure::TaskInjection
      || failure == TapeSessionFailure::WorkerSignalling || failure == TapeSessionFailure::UnexpectedCleanup
      || failure == TapeSessionFailure::UnclassifiedFile;
    check(!propagationOnly);
  }

  for (size_t i = 0; i < static_cast<size_t>(TapeSessionEvent::Count); ++i) {
    SCOPED_TRACE(i);
    tracker.beginTapeSession(cta::tape::session::SessionType::Retrieve);
    tracker.recordEvent(static_cast<TapeSessionEvent>(i));
    EXPECT_FALSE(tracker.hasFailures());
    check(true);
  }

  tracker.beginTapeSession(cta::tape::session::SessionType::Retrieve);
  tracker.incrementTapeAlert(0x01);
  EXPECT_FALSE(tracker.hasFailures());
  check(true);

  tracker.beginTapeSession(cta::tape::session::SessionType::Retrieve);
  tracker.recordFailure(TapeSessionFailure::UnexpectedSession);
  tracker.recordFailure(TapeSessionFailure::DiskRead);
  check(true);
  tracker.beginTapeSession(cta::tape::session::SessionType::Retrieve);
  check(false);
}

TEST_F(cta_tape_daemon_RecallReportPackerTest, RecallReportPackerNominal) {
  auto catalogue = cta::catalogue::DummyCatalogue();
  cta::MockRetrieveMount retrieveMount(catalogue);

  ::testing::InSequence dummy;
  std::unique_ptr<cta::RetrieveJob> job1;
  int job1completes(0), job1failures(0);
  {
    std::unique_ptr<cta::MockRetrieveJob> mockJob(
      new MockRetrieveJobExternalStats(retrieveMount, job1completes, job1failures));
    job1.reset(mockJob.release());
  }
  int job2completes(0), job2failures(0);
  std::unique_ptr<cta::RetrieveJob> job2;
  {
    std::unique_ptr<cta::MockRetrieveJob> mockJob(
      new MockRetrieveJobExternalStats(retrieveMount, job2completes, job2failures));
    job2.reset(mockJob.release());
  }

  cta::log::StringLogger log("dummy", "cta_tape_RecallReportPackerNominal", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  cta::tape::daemon::TapeSessionTracker rrpTracker;
  cta::tape::daemon::RecallReportPacker rrp(&retrieveMount, lc, rrpTracker);
  rrp.startThreads();

  rrp.reportCompletedJob(std::move(job1), lc);
  rrp.reportCompletedJob(std::move(job2), lc);

  rrp.setTapeDone();
  rrp.setDiskDone();
  rrp.reportEndOfSession(lc);
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Nominal RecallReportPacker::EndofSession has been reported"));
  ASSERT_EQ(1, job1completes);
  ASSERT_EQ(1, job2completes);
  ASSERT_EQ(1, retrieveMount.completes);
}

TEST_F(cta_tape_daemon_RecallReportPackerTest, RecallReportPackerBadBadEnd) {
  auto catalogue = cta::catalogue::DummyCatalogue();
  cta::MockRetrieveMount retrieveMount(catalogue);

  ::testing::InSequence dummy;
  std::unique_ptr<cta::RetrieveJob> job1;
  int job1completes(0), job1failures(0);
  {
    std::unique_ptr<cta::MockRetrieveJob> mockJob(
      new MockRetrieveJobExternalStats(retrieveMount, job1completes, job1failures));
    job1.reset(mockJob.release());
  }
  int job2completes(0), job2failures(0);
  std::unique_ptr<cta::RetrieveJob> job2;
  {
    std::unique_ptr<cta::MockRetrieveJob> mockJob(
      new MockRetrieveJobExternalStats(retrieveMount, job2completes, job2failures));
    job2.reset(mockJob.release());
  }
  int job3completes(0), job3failures(0);
  std::unique_ptr<cta::RetrieveJob> job3;
  {
    std::unique_ptr<cta::MockRetrieveJob> mockJob(
      new MockRetrieveJobExternalStats(retrieveMount, job3completes, job3failures));
    job3.reset(mockJob.release());
  }

  cta::log::StringLogger log("dummy", "cta_tape_RecallReportPackerBadBadEnd", cta::log::DEBUG);
  cta::log::LogContext lc(log);

  cta::tape::daemon::TapeSessionTracker rrpTracker;
  cta::tape::daemon::RecallReportPacker rrp(&retrieveMount, lc, rrpTracker);
  rrp.startThreads();

  rrp.reportCompletedJob(std::move(job1), lc);
  rrp.reportCompletedJob(std::move(job2), lc);

  const std::string error_msg = "ERROR_TEST_MSG";
  const cta::exception::Exception ex(error_msg);
  rrpTracker.recordFailure(cta::tape::daemon::TapeSessionFailure::DiskRead);
  const auto failure = rrpTracker.recordFailure(cta::tape::daemon::TapeSessionFailure::UnclassifiedFile);
  rrp.reportFailedJob(std::move(job3), ex, lc, failure);

  rrp.setTapeDone();
  rrp.setDiskDone();
  rrp.reportEndOfSession(lc);
  rrp.waitThread();
  EXPECT_TRUE(rrpTracker.hasFailures());
  EXPECT_EQ(1,
            rrpTracker.outcomeSnapshot()
              .failures[static_cast<size_t>(cta::tape::daemon::TapeSessionFailure::UnclassifiedFile)]);
  EXPECT_EQ(
    1,
    rrpTracker.outcomeSnapshot().failures[static_cast<size_t>(cta::tape::daemon::TapeSessionFailure::DiskRead)]);

  const std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find(error_msg));
  ASSERT_EQ(1, job1completes);
  ASSERT_EQ(1, job2completes);
  ASSERT_EQ(1, job3failures);
  ASSERT_EQ(1, retrieveMount.completes);
}

TEST_F(cta_tape_daemon_RecallReportPackerTest, RejectsFailureFromAnotherTracker) {
  cta::catalogue::DummyCatalogue catalogue;
  cta::MockRetrieveMount mount(catalogue);
  cta::log::StringLogger log("dummy", "wrongTracker", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  cta::tape::daemon::TapeSessionTracker tracker;
  cta::tape::daemon::TapeSessionTracker other;
  cta::tape::daemon::RecallReportPacker reporter(&mount, lc, tracker);
  const auto failure = other.recordFailure(cta::tape::daemon::TapeSessionFailure::DiskWrite);
  EXPECT_THROW(reporter.reportFailedJob(nullptr, cta::exception::Exception("failed"), lc, failure), std::logic_error);
  EXPECT_FALSE(tracker.hasFailures());
}

}  // namespace unitTests
