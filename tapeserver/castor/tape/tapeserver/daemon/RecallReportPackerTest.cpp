/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/StringLogger.hpp"
#include "scheduler/testingMocks/MockRetrieveJob.hpp"
#include "scheduler/testingMocks/MockRetrieveMount.hpp"

using ::testing::_;
using ::testing::Invoke;
namespace unitTests{

class castor_tape_tapeserver_daemon_RecallReportPackerTest: public ::testing::Test {
protected:

  void SetUp() {
  }

  void TearDown() {
  }

}; // class castor_tape_tapeserver_daemon_RecallReportPackerTest

  class MockRetrieveJobExternalStats: public cta::MockRetrieveJob {
  public:
    MockRetrieveJobExternalStats(cta::RetrieveMount & rm, int & completes, int &failures):
    MockRetrieveJob(rm), completesRef(completes), failuresRef(failures) {}

    void asyncSetSuccessful() override {
      completesRef++;
    }

    void transferFailed(const std::string &failureReason, cta::log::LogContext&) override {
      failuresRef++;
    }

  private:
    int & completesRef;
    int & failuresRef;
  };

TEST_F(castor_tape_tapeserver_daemon_RecallReportPackerTest, RecallReportPackerNominal) {
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

  cta::log::StringLogger log("dummy","castor_tape_tapeserver_RecallReportPackerNominal",cta::log::DEBUG, "configFilename");
  cta::log::LogContext lc(log);
  castor::tape::tapeserver::daemon::RecallReportPacker rrp(&retrieveMount,lc);
  rrp.startThreads();

  rrp.reportCompletedJob(std::move(job1), lc);
  rrp.reportCompletedJob(std::move(job2), lc);

  rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting, std::nullopt, lc);

  rrp.setTapeDone();
  rrp.setDiskDone();
  rrp.reportEndOfSession(lc);
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Nominal RecallReportPacker::EndofSession has been reported"));
  ASSERT_EQ(1,job1completes);
  ASSERT_EQ(1,job2completes);
  ASSERT_EQ(1,retrieveMount.completes);
}

TEST_F(castor_tape_tapeserver_daemon_RecallReportPackerTest, RecallReportPackerBadBadEnd) {
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

  cta::log::StringLogger log("dummy","castor_tape_tapeserver_RecallReportPackerBadBadEnd",cta::log::DEBUG, "configFilename");
  cta::log::LogContext lc(log);

  castor::tape::tapeserver::daemon::RecallReportPacker rrp(&retrieveMount,lc);
  rrp.startThreads();

  rrp.reportCompletedJob(std::move(job1), lc);
  rrp.reportCompletedJob(std::move(job2), lc);

  const std::string error_msg = "ERROR_TEST_MSG";
  const cta::exception::Exception ex(error_msg);
  rrp.reportFailedJob(std::move(job3), ex, lc);
  
  rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting, std::nullopt, lc);

  rrp.setTapeDone();
  rrp.setDiskDone();
  rrp.reportEndOfSession(lc);
  rrp.waitThread();

  const std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find(error_msg));
  ASSERT_EQ(1, job1completes);
  ASSERT_EQ(1, job2completes);
  ASSERT_EQ(1, job3failures);
  ASSERT_EQ(1, retrieveMount.completes);
}

}
