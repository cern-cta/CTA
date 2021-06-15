/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "common/log/StringLogger.hpp"
#include "common/exception/Exception.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#include "objectstore/BackendVFS.hpp"
#include "scheduler/testingMocks/MockRetrieveMount.hpp"
#include "scheduler/testingMocks/MockRetrieveJob.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

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

  cta::log::StringLogger log("dummy","castor_tape_tapeserver_RecallReportPackerNominal",cta::log::DEBUG);
  cta::log::LogContext lc(log);
  castor::tape::tapeserver::daemon::RecallReportPacker rrp(&retrieveMount,lc);
  rrp.startThreads();

  rrp.reportCompletedJob(std::move(job1));
  rrp.reportCompletedJob(std::move(job2));
  
  rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting);
  
  rrp.reportEndOfSession();
//  rrp.reportTestGoingToEnd();
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

  cta::log::StringLogger log("dummy","castor_tape_tapeserver_RecallReportPackerBadBadEnd",cta::log::DEBUG);
  cta::log::LogContext lc(log);

  castor::tape::tapeserver::daemon::RecallReportPacker rrp(&retrieveMount,lc);
  rrp.startThreads();

  rrp.reportCompletedJob(std::move(job1));
  rrp.reportCompletedJob(std::move(job2));

  const std::string error_msg = "ERROR_TEST_MSG";
  const cta::exception::Exception ex(error_msg);
  rrp.reportFailedJob(std::move(job3), ex);
  
  rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting);

  rrp.reportEndOfSession();
//  rrp.reportTestGoingToEnd();
  rrp.waitThread();

  const std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find(error_msg));
  ASSERT_EQ(1, job1completes);
  ASSERT_EQ(1, job2completes);
  ASSERT_EQ(1, job3failures);
  ASSERT_EQ(1, retrieveMount.completes);
}

}
