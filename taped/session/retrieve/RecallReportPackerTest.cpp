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
#ifndef CTA_PGSCHED
  // In pgsched mode, transferFailed() is never called — the packer
  // accumulates jobs and delegates to failTransferBatch() instead.
  ASSERT_EQ(1, job3failures);
#endif
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

#ifdef CTA_PGSCHED

class MockRetrieveJobWithPrepare : public cta::MockRetrieveJob {
public:
  int prepareCount = 0;

  explicit MockRetrieveJobWithPrepare(cta::RetrieveMount& rm) : cta::MockRetrieveJob(rm) {}

  void prepareFailTransfer(const std::string&) override { prepareCount++; }
};

class MockRetrieveMountWithBatch : public cta::MockRetrieveMount {
public:
  int flushCount = 0;
  size_t lastBatchSize = 0;
  int successCount = 0;
  int failureCount = 0;

  explicit MockRetrieveMountWithBatch(cta::catalogue::Catalogue& catalogue) : cta::MockRetrieveMount(catalogue) {}

  void setJobBatchTransferred(std::queue<std::unique_ptr<cta::RetrieveJob>>& successfulRetrieveJobs,
                              cta::log::LogContext&) override {
    while (!successfulRetrieveJobs.empty()) {
      successCount++;
      successfulRetrieveJobs.pop();
    }
  }

  void failTransferBatch(std::vector<std::unique_ptr<cta::RetrieveJob>>& jobs, cta::log::LogContext&) override {
    flushCount++;
    lastBatchSize = jobs.size();
    failureCount += static_cast<int>(jobs.size());
  }
};

TEST_F(cta_tape_daemon_RecallReportPackerTest, RecallReportPackerPgschedAccumulateSmallBatch) {
  auto catalogue = cta::catalogue::DummyCatalogue();
  MockRetrieveMountWithBatch retrieveMount(catalogue);

  cta::log::StringLogger log("dummy", "RecallReportPackerPgschedAccumulateSmallBatch", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  cta::tape::daemon::TapeSessionTracker rrpTracker;
  cta::tape::daemon::RecallReportPacker rrp(&retrieveMount, lc, rrpTracker);
  rrp.startThreads();

  // 3 failed jobs — well below c_failedJobsBatchMax, so no mid-session flush
  for (int i = 0; i < 3; i++) {
    std::unique_ptr<cta::RetrieveJob> job(new MockRetrieveJobWithPrepare(retrieveMount));
    const cta::exception::Exception ex("failure " + std::to_string(i));
    const auto failure = rrpTracker.recordFailure(cta::tape::daemon::TapeSessionFailure::UnclassifiedFile);
    rrp.reportFailedJob(std::move(job), ex, lc, failure);
  }

  rrp.setTapeDone();
  rrp.setDiskDone();
  rrp.reportEndOfSession(lc);
  rrp.waitThread();

  // End-of-session triggers exactly one failTransferBatch call with all 3 jobs
  ASSERT_EQ(1, retrieveMount.flushCount);
  ASSERT_EQ(3, retrieveMount.failureCount);
  ASSERT_EQ(0, retrieveMount.successCount);
  ASSERT_EQ(1, retrieveMount.completes);
}

TEST_F(cta_tape_daemon_RecallReportPackerTest, RecallReportPackerPgschedBatchSizeFlush) {
  auto catalogue = cta::catalogue::DummyCatalogue();
  MockRetrieveMountWithBatch retrieveMount(catalogue);

  cta::log::StringLogger log("dummy", "RecallReportPackerPgschedBatchSizeFlush", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  cta::tape::daemon::TapeSessionTracker rrpTracker;
  cta::tape::daemon::RecallReportPacker rrp(&retrieveMount, lc, rrpTracker);
  rrp.startThreads();

  // Send 601 failed jobs: the 600th triggers an immediate mid-session flush,
  // leaving 1 job accumulated for the end-of-session flush.
  // 600 matches c_failedJobsBatchMax in RecallReportPacker.
  for (int i = 0; i < 601; i++) {
    std::unique_ptr<cta::RetrieveJob> job(new MockRetrieveJobWithPrepare(retrieveMount));
    const cta::exception::Exception ex("failure " + std::to_string(i));
    const auto failure = rrpTracker.recordFailure(cta::tape::daemon::TapeSessionFailure::UnclassifiedFile);
    rrp.reportFailedJob(std::move(job), ex, lc, failure);
  }

  rrp.setTapeDone();
  rrp.setDiskDone();
  rrp.reportEndOfSession(lc);
  rrp.waitThread();

  // Two flushes: one mid-session at batch size 600, one at end-of-session with remaining 1
  ASSERT_EQ(2, retrieveMount.flushCount);
  ASSERT_EQ(601, retrieveMount.failureCount);
  ASSERT_EQ(0, retrieveMount.successCount);
  ASSERT_EQ(1, retrieveMount.completes);
}

TEST_F(cta_tape_daemon_RecallReportPackerTest, RecallReportPackerPgschedFailuresPiggybackOnSuccessFlush) {
  auto catalogue = cta::catalogue::DummyCatalogue();
  MockRetrieveMountWithBatch retrieveMount(catalogue);

  cta::log::StringLogger log("dummy", "RecallReportPackerPgschedFailuresPiggybackOnSuccessFlush", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  cta::tape::daemon::TapeSessionTracker rrpTracker;
  cta::tape::daemon::RecallReportPacker rrp(&retrieveMount, lc, rrpTracker);
  rrp.startThreads();

  // 3 failures — below c_failedJobsBatchMax (600), so no batch-size flush mid-session
  for (int i = 0; i < 3; i++) {
    std::unique_ptr<cta::RetrieveJob> job(new MockRetrieveJobWithPrepare(retrieveMount));
    const cta::exception::Exception ex("failure " + std::to_string(i));
    const auto failure = rrpTracker.recordFailure(cta::tape::daemon::TapeSessionFailure::UnclassifiedFile);
    rrp.reportFailedJob(std::move(job), ex, lc, failure);
  }

  // RECALL_REPORT_PACKER_FLUSH_SIZE (2000) successful jobs trigger fullCheckAndFinishAsyncExecute(),
  // which now also calls flushFailedJobs() — the piggyback added in this branch.
  // MockRetrieveJob::asyncSetSuccessful() does not throw, so all 2000 reach m_successfulRetrieveJobs.
  for (unsigned int i = 0; i < 2000; i++) {
    std::unique_ptr<cta::RetrieveJob> job(new MockRetrieveJobWithPrepare(retrieveMount));
    rrp.reportCompletedJob(std::move(job), lc);
  }

  rrp.setTapeDone();
  rrp.setDiskDone();
  rrp.reportEndOfSession(lc);
  rrp.waitThread();

  // Failures are flushed mid-session by the success-batch piggyback (once 2000 successes
  // accumulate, fullCheckAndFinishAsyncExecute flushes both); end-of-session finds both
  // accumulators empty and produces no additional calls.
  ASSERT_EQ(1, retrieveMount.flushCount);
  ASSERT_EQ(3, retrieveMount.failureCount);
  ASSERT_EQ(2000, retrieveMount.successCount);
  ASSERT_EQ(1, retrieveMount.completes);
}

#endif  // CTA_PGSCHED

}  // namespace unitTests
