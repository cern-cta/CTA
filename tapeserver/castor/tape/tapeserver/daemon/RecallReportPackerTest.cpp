/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/log/StringLogger.hpp"
#include "serrno.h"
#include "scheduler/mockDB/MockSchedulerDatabase.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using ::testing::_;
using ::testing::Invoke;
namespace unitTests{

class castor_tape_tapeserver_daemonTest: public ::testing::Test {
protected:

  void SetUp() {
  }

  void TearDown() {
  }

  class MockRetrieveJob: public cta::RetrieveJob {
  public:
    MockRetrieveJob() {
    }

    ~MockRetrieveJob() throw() {
    }
    

    MOCK_METHOD2(complete, void(const uint32_t checksumOfTransfer, const uint64_t fileSizeOfTransfer));
  }; // class MockRetrieveJob

  class MockRetrieveMount: public cta::RetrieveMount {
  public:
    MockRetrieveMount() {
      const unsigned int nbRecallJobs = 2;
      createRetrieveJobs(nbRecallJobs);
    }

    ~MockRetrieveMount() throw() {
    }

    std::unique_ptr<cta::RetrieveJob> getNextJob() {
      internalGetNextJob();
      if(m_jobs.empty()) {
        return std::unique_ptr<cta::RetrieveJob>();
      } else {
        std::unique_ptr<cta::RetrieveJob> job =  std::move(m_jobs.front());
        m_jobs.pop_front();
        return job;
      }
    }

    MOCK_METHOD0(internalGetNextJob, cta::RetrieveJob*());
    MOCK_METHOD0(complete, void());

  private:

    std::list<std::unique_ptr<cta::RetrieveJob>> m_jobs;

    void createRetrieveJobs(const unsigned int nbJobs) {
      for(unsigned int i = 0; i < nbJobs; i++) {
        std::unique_ptr<MockRetrieveJob> job(new MockRetrieveJob());
        EXPECT_CALL(*job, complete(_,_)).Times(1);
        m_jobs.push_back(std::unique_ptr<cta::RetrieveJob>(
          job.release()));
      }
    }
  }; // class MockRetrieveMount

}; // class castor_tape_tapeserver_daemonTest

TEST_F(castor_tape_tapeserver_daemonTest, RecallReportPackerNominal) {
  MockRetrieveMount retrieveMount;

  ::testing::InSequence dummy;
  EXPECT_CALL(retrieveMount, internalGetNextJob()).Times(3);
  EXPECT_CALL(retrieveMount, complete()).Times(1);

  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerNominal");
  castor::log::LogContext lc(log);
  castor::tape::tapeserver::daemon::RecallReportPacker rrp(dynamic_cast<cta::RetrieveMount *>(&retrieveMount),lc);
  rrp.startThreads();
  
  std::unique_ptr<cta::RetrieveJob> job1 = retrieveMount.getNextJob();
  std::unique_ptr<cta::RetrieveJob> job2 = retrieveMount.getNextJob();
  std::unique_ptr<cta::RetrieveJob> job3 = retrieveMount.getNextJob();

  ASSERT_EQ(NULL, job3.get());

  rrp.reportCompletedJob(std::move(job1),0,0);
  rrp.reportCompletedJob(std::move(job2),0,0);
  rrp.reportEndOfSession();
  rrp.waitThread();
  
  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Nominal RecallReportPacker::EndofSession has been reported"));
}

/*
TEST_F(castor_tape_tapeserver_daemonTest, RecallReportPackerCumulated) {
  MockClient client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportRecallResults(_,_)).Times(2);
  EXPECT_CALL(client, reportEndOfSession(_)).Times(1);
  
  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerCumulated");
  castor::log::LogContext lc(log);
  std::unique_ptr<cta::MockSchedulerDatabase> mdb(new cta::MockSchedulerDatabase);
  tapeserver::daemon::RecallReportPacker rrp(dynamic_cast<cta::RetrieveMount *>((mdb->getNextMount("ll","drive")).get()),lc);
  rrp.startThreads();
  
  tapegateway::FileToRecallStruct recalledFiled;
  rrp.reportCompletedJob(recalledFiled,0,0);
  rrp.reportCompletedJob(recalledFiled,0,0);
  rrp.reportCompletedJob(recalledFiled,0,0);
  rrp.reportEndOfSession();
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Nominal RecallReportPacker::EndofSession has been reported"));
}

TEST_F(castor_tape_tapeserver_daemonTest, RecallReportPackerBadBadEnd) {
  std::string error_msg="ERROR_TEST_MSG";
  int error_code=std::numeric_limits<int>::max();
  //FakeClient client;
  MockClient client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportRecallResults(_,_)).Times(2);
  EXPECT_CALL(client, reportEndOfSessionWithError(error_msg,error_code,_)).Times(1);
  
  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerBadBadEnd");
  castor::log::LogContext lc(log);

  std::unique_ptr<cta::MockSchedulerDatabase> mdb(new cta::MockSchedulerDatabase);
  tapeserver::daemon::RecallReportPacker rrp(dynamic_cast<cta::RetrieveMount *>((mdb->getNextMount("ll","drive")).get()),lc);
  rrp.startThreads();

  tapegateway::FileToRecallStruct recalledFiled;
  rrp.reportCompletedJob(recalledFiled,0,0);
  rrp.reportCompletedJob(recalledFiled,0,0);
  rrp.reportCompletedJob(recalledFiled,0,0);
  rrp.reportFailedJob(recalledFiled,error_msg,error_code);
  rrp.reportEndOfSessionWithErrors(error_msg,error_code);
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find(error_msg));
}

TEST_F(castor_tape_tapeserver_daemonTest, RecallReportPackerBadGoodEnd) {

  std::string error_msg="ERROR_TEST_MSG";  

  MockClient client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportRecallResults(_,_)).Times(2);
  EXPECT_CALL(client, 
    reportEndOfSessionWithError("RecallReportPacker::EndofSession has been reported  but an error happened somewhere in the process",SEINTERNAL,_)
          ).Times(1);

  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerBadBadEnd");
  castor::log::LogContext lc(log);
  
  std::unique_ptr<cta::MockSchedulerDatabase> mdb(new cta::MockSchedulerDatabase);
  tapeserver::daemon::RecallReportPacker rrp(dynamic_cast<cta::RetrieveMount *>((mdb->getNextMount("ll","drive")).get()),lc);
  rrp.startThreads();
  
  
  tapegateway::FileToRecallStruct recalledFiled;
  rrp.reportCompletedJob(recalledFiled,0,0);
  rrp.reportCompletedJob(recalledFiled,0,0);
  rrp.reportCompletedJob(recalledFiled,0,0);
  rrp.reportFailedJob(recalledFiled,error_msg,-1);
  rrp.reportEndOfSession();
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("EndofSession has been reported  but an error happened somewhere in the process"));
}
TEST_F(castor_tape_tapeserver_daemonTest, RecallReportPackerGoodBadEnd) {
  
  std::string error_msg="ERROR_TEST_MSG";
  int error_code=std::numeric_limits<int>::max();
  
  MockClient client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportRecallResults(_,_)).Times(1);
  EXPECT_CALL(client, 
          reportEndOfSessionWithError("RecallReportPacker::EndofSessionWithErrors has been reported  but NO error was detected during the process",SEINTERNAL,_)
          ).Times(1);

  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerBadBadEnd");
  castor::log::LogContext lc(log);
  
  std::unique_ptr<cta::MockSchedulerDatabase> mdb(new cta::MockSchedulerDatabase);
  tapeserver::daemon::RecallReportPacker rrp(dynamic_cast<cta::RetrieveMount *>((mdb->getNextMount("ll","drive")).get()),lc);
  rrp.startThreads();
  
  tapegateway::FileToRecallStruct recalledFiled;
  rrp.reportCompletedJob(recalledFiled,0,0);
  rrp.reportCompletedJob(recalledFiled,0,0);
  rrp.reportEndOfSessionWithErrors(error_msg,error_code);
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("EndofSessionWithErrors has been reported  but NO error was detected during the process"));
}

TEST_F(castor_tape_tapeserver_daemonTest, RecallReportPackerFaillure) {
  
  std::string error_msg="ERROR_TEST_MSG";
  int error_code=std::numeric_limits<int>::max();
  
  using namespace ::testing;
  
  MockClient client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportRecallResults(_,_)).WillRepeatedly(Throw(castor::exception::Exception("")));
  EXPECT_CALL(client, reportEndOfSessionWithError(_,SEINTERNAL,_)).Times(1);

  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerBadBadEnd");
  castor::log::LogContext lc(log);
  
  std::unique_ptr<cta::MockSchedulerDatabase> mdb(new cta::MockSchedulerDatabase);
  tapeserver::daemon::RecallReportPacker rrp(dynamic_cast<cta::RetrieveMount *>((mdb->getNextMount("ll","drive")).get()),lc);
  rrp.startThreads();
  
  tapegateway::FileToRecallStruct recalledFiled;
  rrp.reportCompletedJob(recalledFiled,0,0);
  rrp.reportCompletedJob(recalledFiled,0,0);
  rrp.reportEndOfSessionWithErrors(error_msg,error_code);
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Successfully closed client's session after the failed report RecallResult"));
}
*/
}
